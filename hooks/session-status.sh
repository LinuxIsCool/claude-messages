#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${HOME}/.claude/local/messages"
DB="${DATA_DIR}/messages.db"

# Check daemon status
if systemctl --user is-active legion-messages &>/dev/null; then
  DAEMON_STATUS="running"
else
  DAEMON_STATUS="stopped"
fi

# Get message counts if DB exists
if [ -f "$DB" ]; then
  TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM messages;" 2>/dev/null || echo "0")
  PLATFORMS=$(sqlite3 "$DB" "SELECT platform || ':' || COUNT(*) FROM messages GROUP BY platform;" 2>/dev/null | tr '\n' ' ' || echo "none")
  THREADS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM threads;" 2>/dev/null || echo "0")

  echo "[messages] daemon: ${DAEMON_STATUS} · ${TOTAL} messages (${PLATFORMS}) · ${THREADS} threads"
else
  echo "[messages] daemon: ${DAEMON_STATUS} · no database yet"
fi

exit 0
