#!/usr/bin/env bash
# check-messages-health.sh — Read health.json, alert on stale or failing adapters
# Called by systemd timer every 5 minutes.
#
# Exit codes:
#   0 = all adapters healthy
#   1 = one or more adapters stale or failing
#   2 = health.json missing or unreadable (daemon likely down)
#
# Health is a loop, not a guess: if consecutive_failures >= threshold, the adapter
# is DEAD regardless of how recently last_success fired. A stale last_success is
# a warning; a rising consecutive_failures counter is a fact.

set -euo pipefail

HEALTH_FILE="${HOME}/.claude/local/messages/health.json"
ALERT_LOG="${HOME}/.claude/local/messages/health-alerts.log"
GMAIL_SCHEDULE_RECEIPT="${HOME}/.claude/local/calendar/gmail-schedule-observation.json"
REQUIRE_GMAIL_SCHEDULE="${REQUIRE_GMAIL_SCHEDULE:-1}"

# Staleness thresholds per tier (seconds)
# Tier-0 adapters sync in <1s but the full cycle takes ~8 min (Telegram scans 881 dialogs).
# Threshold must exceed: cycle_duration + poll_interval + margin.
TIER_0_THRESHOLD=1200   # 20 min — ~2.5 full cycles (Telegram scan ~8min + poll ~60s + variance)
TIER_2_THRESHOLD=7200   # 2 hr   — network adapters (Telegram, Email, Slack)

# Daemon-level staleness: if last_cycle is older than this, daemon is stuck/dead
DAEMON_THRESHOLD=1200   # 20 min (cycle ~8min + poll ~60s + generous margin)

# Failure escalation: how many consecutive failures before declaring the adapter dead
# (independent of staleness). A single fresh cycle that fails is noise; 2+ is a pattern.
FAILURE_THRESHOLD=1
SIGNAL_SOURCE_THRESHOLD=300  # authenticated websocket evidence must be recent
GMAIL_SCHEDULE_THRESHOLD=600

NOW_EPOCH=$(date +%s)

# --- Check health.json exists ---
if [[ ! -f "$HEALTH_FILE" ]]; then
  MSG="legion-messages health.json missing — daemon may not be running"
  mkdir -p "$(dirname "$ALERT_LOG")" 2>/dev/null || true
  echo "$(date -Iseconds) CRITICAL $MSG" >> "$ALERT_LOG"
  timeout 5s notify-send -u critical "Messages Daemon" "$MSG" 2>/dev/null || true
  exit 2
fi

# --- Parse with jq ---
HEALTH=$(cat "$HEALTH_FILE")
LAST_CYCLE=$(echo "$HEALTH" | jq -r '.last_cycle')
STARTED_AT=$(echo "$HEALTH" | jq -r '.started_at')
DAEMON_NAME=$(echo "$HEALTH" | jq -r '.daemon')

# Convert last_cycle to epoch
LAST_CYCLE_EPOCH=$(date -d "$LAST_CYCLE" +%s 2>/dev/null || echo 0)
DAEMON_AGE=$((NOW_EPOCH - LAST_CYCLE_EPOCH))
STARTED_EPOCH=$(date -d "$STARTED_AT" +%s 2>/dev/null || echo 0)
DAEMON_UPTIME=$((NOW_EPOCH - STARTED_EPOCH))

# --- Check daemon-level staleness ---
if [[ $DAEMON_AGE -gt $DAEMON_THRESHOLD ]]; then
  MSG="${DAEMON_NAME} last cycle ${DAEMON_AGE}s ago (threshold: ${DAEMON_THRESHOLD}s) — daemon may be stuck"
  echo "$(date -Iseconds) CRITICAL $MSG" >> "$ALERT_LOG"
  timeout 5s notify-send -u critical "Messages Daemon" "$MSG" 2>/dev/null || true
  exit 1
fi

# --- Check per-adapter staleness AND consecutive failures ---
# Both checks fire independently. Staleness catches cold-start adapters that
# have never synced. consecutive_failures catches adapters that were alive and
# then died (the Sep 5–8 telegram case: last_success was fresh-ish but failures
# kept incrementing through cooldowns, and the old script never read that field).
STALE_COUNT=0
FAIL_COUNT=0
STALE_ADAPTERS=""
FAIL_ADAPTERS=""

for PLATFORM in $(echo "$HEALTH" | jq -r '.adapters | keys[]'); do
  ADAPTER=$(echo "$HEALTH" | jq ".adapters[\"$PLATFORM\"]")
  TIER=$(echo "$ADAPTER" | jq -r '.tier')
  LAST_SUCCESS=$(echo "$ADAPTER" | jq -r '.last_success')
  CONSECUTIVE_FAILURES=$(echo "$ADAPTER" | jq -r '.consecutive_failures')
  LAST_ERROR=$(echo "$ADAPTER" | jq -r '.last_error')
  TIMED_OUT=$(echo "$ADAPTER" | jq -r '.timed_out')

  # Signal's local database remains readable when Signal Desktop is offline.
  # Require independent evidence from its authenticated websocket, so an empty
  # poll against a frozen cache cannot make the lane green.
  if [[ "$PLATFORM" == "signal" ]]; then
    SOURCE_OBSERVED_AT=$(echo "$ADAPTER" | jq -r '.source_observed_at // "null"')
    SOURCE_ERROR=""
    if [[ "$SOURCE_OBSERVED_AT" == "null" ]]; then
      SOURCE_ERROR="no source observation"
    else
      SOURCE_EPOCH=$(date -d "$SOURCE_OBSERVED_AT" +%s 2>/dev/null || echo 0)
      SOURCE_AGE=$((NOW_EPOCH - SOURCE_EPOCH))
      if [[ $SOURCE_AGE -gt $SIGNAL_SOURCE_THRESHOLD ]]; then
        SOURCE_ERROR="source observation ${SOURCE_AGE}s old"
      fi
    fi
    if [[ -n "$SOURCE_ERROR" && "$CONSECUTIVE_FAILURES" -lt "$FAILURE_THRESHOLD" ]]; then
      FAIL_COUNT=$((FAIL_COUNT + 1))
      FAIL_ADAPTERS="${FAIL_ADAPTERS} signal(${SOURCE_ERROR})"
    fi
  fi

  # Determine staleness threshold
  if [[ "$TIER" -eq 0 ]]; then
    THRESHOLD=$TIER_0_THRESHOLD
  else
    THRESHOLD=$TIER_2_THRESHOLD
  fi

  # --- Staleness check: adapter hasn't succeeded within threshold ---
  if [[ "$LAST_SUCCESS" == "null" ]]; then
    # Never succeeded — alert if daemon has been up long enough
    if [[ $DAEMON_UPTIME -gt $THRESHOLD ]]; then
      STALE_COUNT=$((STALE_COUNT + 1))
      STALE_ADAPTERS="${STALE_ADAPTERS} ${PLATFORM}(never-synced)"
    fi
  else
    # Has succeeded at some point — check how long ago
    SUCCESS_EPOCH=$(date -d "$LAST_SUCCESS" +%s 2>/dev/null || echo 0)
    AGE=$((NOW_EPOCH - SUCCESS_EPOCH))

    if [[ $AGE -gt $THRESHOLD ]]; then
      STALE_COUNT=$((STALE_COUNT + 1))
      STALE_ADAPTERS="${STALE_ADAPTERS} ${PLATFORM}(${AGE}s,tier${TIER})"
    fi
  fi

  # --- Consecutive failures check: independent failure signal ---
  # This catches adapters that were alive and then died, regardless of
  # when last_success fired. Even if last_success is within threshold,
  # if failures are stacking, the lane is dead.
  if [[ "$CONSECUTIVE_FAILURES" -ge "$FAILURE_THRESHOLD" ]]; then
    FAIL_COUNT=$((FAIL_COUNT + 1))
    # Include error message if available (truncate long ones)
    ERROR_SNIPPET=""
    if [[ "$LAST_ERROR" != "null" && -n "$LAST_ERROR" ]]; then
      ERROR_SNIPPET=" (error: $(echo "$LAST_ERROR" | head -c 80))"
    fi
    TIMEOUT_FLAG=""
    if [[ "$TIMED_OUT" == "true" ]]; then
      TIMEOUT_FLAG=" [TIMED_OUT]"
    fi
    FAIL_ADAPTERS="${FAIL_ADAPTERS} ${PLATFORM}(failures=${CONSECUTIVE_FAILURES}${TIMEOUT_FLAG}${ERROR_SNIPPET})"
  fi
done

# Gmail is the authoritative mailbox for invitation and cancellation messages.
# Its derived schedule is a separate observation lane and must not inherit green
# status from email ingestion unless the projection is also fresh and complete.
if [[ "$REQUIRE_GMAIL_SCHEDULE" == "1" ]]; then
  SCHEDULE_ERROR=""
  if [[ ! -f "$GMAIL_SCHEDULE_RECEIPT" ]]; then
    SCHEDULE_ERROR="missing receipt"
  else
    SCHEDULE_STATUS=$(jq -r '.status // "unknown"' "$GMAIL_SCHEDULE_RECEIPT" 2>/dev/null || echo "invalid")
    SCHEDULE_PROJECTED_AT=$(jq -r '.projection_at // "null"' "$GMAIL_SCHEDULE_RECEIPT" 2>/dev/null || echo "null")
    SCHEDULE_SOURCE_AT=$(jq -r '.source_contact_at // "null"' "$GMAIL_SCHEDULE_RECEIPT" 2>/dev/null || echo "null")
    if [[ "$SCHEDULE_STATUS" != "ok" ]]; then
      SCHEDULE_ERROR="status=${SCHEDULE_STATUS}"
    elif [[ "$SCHEDULE_PROJECTED_AT" == "null" || "$SCHEDULE_SOURCE_AT" == "null" ]]; then
      SCHEDULE_ERROR="missing projection or source evidence"
    else
      SCHEDULE_EPOCH=$(date -d "$SCHEDULE_PROJECTED_AT" +%s 2>/dev/null || echo 0)
      SCHEDULE_SOURCE_EPOCH=$(date -d "$SCHEDULE_SOURCE_AT" +%s 2>/dev/null || echo 0)
      SCHEDULE_AGE=$((NOW_EPOCH - SCHEDULE_EPOCH))
      SCHEDULE_SOURCE_AGE=$((NOW_EPOCH - SCHEDULE_SOURCE_EPOCH))
      if [[ $SCHEDULE_AGE -gt $GMAIL_SCHEDULE_THRESHOLD ]]; then
        SCHEDULE_ERROR="projection ${SCHEDULE_AGE}s old"
      elif [[ $SCHEDULE_SOURCE_AGE -gt $GMAIL_SCHEDULE_THRESHOLD ]]; then
        SCHEDULE_ERROR="source observation ${SCHEDULE_SOURCE_AGE}s old"
      fi
    fi
  fi
  if [[ -n "$SCHEDULE_ERROR" ]]; then
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAIL_ADAPTERS="${FAIL_ADAPTERS} gmail-schedule(${SCHEDULE_ERROR})"
  fi
fi

# --- Report failures first (they are more urgent than staleness) ---
if [[ $FAIL_COUNT -gt 0 ]]; then
  MSG="FAIL_COUNT=${FAIL_COUNT} adapter(s) with consecutive failures:${FAIL_ADAPTERS}"
  echo "$(date -Iseconds) CRITICAL $MSG" >> "$ALERT_LOG"
  timeout 5s notify-send -u critical "Messages Daemon" "$MSG" 2>/dev/null || true
  exit 1
fi

# --- Report staleness ---
if [[ $STALE_COUNT -gt 0 ]]; then
  MSG="${STALE_COUNT} stale adapter(s):${STALE_ADAPTERS}"
  echo "$(date -Iseconds) WARNING $MSG" >> "$ALERT_LOG"
  timeout 5s notify-send -u normal "Messages Sync" "$MSG" 2>/dev/null || true
  exit 1
fi

# All healthy — log silently (no notification)
echo "$(date -Iseconds) OK all adapters healthy (cycle ${DAEMON_AGE}s ago)" >> "$ALERT_LOG"
exit 0
