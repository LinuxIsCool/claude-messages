#!/usr/bin/env bash
# test-check-messages-health.sh
# Proves the health check fires RED for consecutive_failures.
# Usage: bash test-check-messages-health.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/check-messages-health.sh"

if [[ ! -f "$SCRIPT_PATH" ]]; then
  echo "FAIL: $SCRIPT_PATH not found"
  exit 2
fi

PASS=0
FAIL=0
report() {
  if [[ "$1" == "$2" ]]; then
    echo "  PASS: $3 (exit=$1)"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $3 (expected=$2, got=$1)"
    FAIL=$((FAIL + 1))
  fi
}

check() {
  local home="$1" expected="$2" label="$3"
  HOME="$home" bash "$SCRIPT_PATH" > /dev/null 2>&1; echo $? > "$TMPDIR/exit"
  local actual
  actual=$(cat "$TMPDIR/exit")
  report "$actual" "$expected" "$label"
}

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

NOW=$(date -Iseconds)
THREE_H=$(date -Iseconds -d '3 hours ago')
FIVE_H=$(date -Iseconds -d '5 hours ago')
ONE_H=$(date -Iseconds -d '1 hour ago')
FIVE_M=$(date -Iseconds -d '5 minutes ago')
TEN_M=$(date -Iseconds -d '10 minutes ago')

write_json() {
  local f="$1"
  shift
  echo '{}' > "$f"
  python3 -c "
import json, sys
h = json.load(open('$f'))
h.update(json.loads('''$(cat | sed "s/|TS|$NOW|g;s/|TH|$THREE_H|g;s/|FIVEH|$FIVE_H|g;s/|ONEH|$ONE_H|g")'''))
json.dump(h, open('$f', 'w'))
" 2>/dev/null || {
    # Fallback: write raw
    cat > "$f"
  }
}

# Test 1: missing health.json -> exit 2
echo "=== Test 1: health.json missing -> exit 2 ==="
check "$TMPDIR/no-health" 2 "missing health.json"

# Test 2: daemon stale -> exit 1
echo ""
echo "=== Test 2: daemon stale (cycle 3h ago) -> exit 1 ==="
mkdir -p "$TMPDIR/t2/.claude/local/messages"
cat > "$TMPDIR/t2/.claude/local/messages/health.json" <<EOF
{
  "daemon": "test", "version": "1.0", "pid": 1,
  "started_at": "$NOW",
  "last_cycle": "$THREE_H",
  "cycle_count": 1, "cycle_duration_ms": 500,
  "adapters": {
    "telegram": {
      "platform": "telegram", "tier": 2,
      "last_success": "$ONE_H",
      "last_failure": null, "last_error": null,
      "last_duration_ms": 5000,
      "last_yield": {"messages":0,"threads":0,"contacts":0},
      "consecutive_failures": 0, "timed_out": false,
      "skipped": false, "cooldown_until": null, "skip_reason": null
    }
  }
}
EOF
check "$TMPDIR/t2" 1 "daemon stale (3h ago)"

# Test 3: THE BUG FIX — consecutive_failures=27 with fresh last_success
echo ""
echo "=== Test 3: consecutive_failures=27, fresh last_success -> exit 1 ==="
mkdir -p "$TMPDIR/t3/.claude/local/messages"
cat > "$TMPDIR/t3/.claude/local/messages/health.json" <<EOF
{
  "daemon": "test", "version": "1.0", "pid": 1,
  "started_at": "$NOW",
  "last_cycle": "$ONE_H",
  "cycle_count": 42, "cycle_duration_ms": 500,
  "adapters": {
    "telegram": {
      "platform": "telegram", "tier": 2,
      "last_success": "$ONE_H",
      "last_failure": "$NOW",
      "last_error": "Timed out after 1800s",
      "last_duration_ms": 1800000,
      "last_yield": {"messages":0,"threads":0,"contacts":0},
      "consecutive_failures": 27, "timed_out": true,
      "skipped": false, "cooldown_until": null, "skip_reason": null
    },
    "signal": {
      "platform": "signal", "tier": 0,
      "last_success": "$ONE_H",
      "last_failure": null, "last_error": null,
      "last_duration_ms": 16,
      "last_yield": {"messages":2,"threads":0,"contacts":0},
      "consecutive_failures": 0, "timed_out": false,
      "source_observed_at": "$NOW",
      "source_evidence": "signal-desktop authenticated websocket keepalive",
      "skipped": false, "cooldown_until": null, "skip_reason": null
    }
  }
}
EOF
check "$TMPDIR/t3" 1 "consecutive_failures=27, last_success fresh"

# Test 4: all healthy -> exit 0
echo ""
echo "=== Test 4: all adapters healthy -> exit 0 ==="
mkdir -p "$TMPDIR/t4/.claude/local/messages"
cat > "$TMPDIR/t4/.claude/local/messages/health.json" <<EOF
{
  "daemon": "test", "version": "1.0", "pid": 1,
  "started_at": "$NOW",
  "last_cycle": "$FIVE_M",
  "cycle_count": 42, "cycle_duration_ms": 500,
  "adapters": {
    "telegram": {
      "platform": "telegram", "tier": 2,
      "last_success": "$ONE_H",
      "last_failure": null, "last_error": null,
      "last_duration_ms": 5000,
      "last_yield": {"messages":10,"threads":926,"contacts":600},
      "consecutive_failures": 0, "timed_out": false,
      "skipped": false, "cooldown_until": null, "skip_reason": null
    },
    "signal": {
      "platform": "signal", "tier": 0,
      "last_success": "$TEN_M",
      "last_failure": null, "last_error": null,
      "last_duration_ms": 16,
      "last_yield": {"messages":2,"threads":0,"contacts":0},
      "consecutive_failures": 0, "timed_out": false,
      "source_observed_at": "$NOW",
      "source_evidence": "signal-desktop authenticated websocket keepalive",
      "skipped": false, "cooldown_until": null, "skip_reason": null
    }
  }
}
EOF
check "$TMPDIR/t4" 0 "all adapters healthy"

# Test 5: stale + consecutive_failures=27 -> exit 1
echo ""
echo "=== Test 5: stale + consecutive_failures=27 -> exit 1 ==="
mkdir -p "$TMPDIR/t5/.claude/local/messages"
cat > "$TMPDIR/t5/.claude/local/messages/health.json" <<EOF
{
  "daemon": "test", "version": "1.0", "pid": 1,
  "started_at": "$NOW",
  "last_cycle": "$FIVE_H",
  "cycle_count": 100, "cycle_duration_ms": 500,
  "adapters": {
    "telegram": {
      "platform": "telegram", "tier": 2,
      "last_success": "$FIVE_H",
      "last_failure": "$NOW",
      "last_error": "Timed out after 1800s",
      "last_duration_ms": 1800000,
      "last_yield": {"messages":0,"threads":0,"contacts":0},
      "consecutive_failures": 27, "timed_out": true,
      "skipped": false, "cooldown_until": null, "skip_reason": null
    }
  }
}
EOF
check "$TMPDIR/t5" 1 "stale + consecutive_failures=27"

# Test 6: a recent empty poll against a frozen local DB must not mask a dead
# Signal Desktop websocket.
echo ""
echo "=== Test 6: stale Signal source with fresh empty poll -> exit 1 ==="
mkdir -p "$TMPDIR/t6/.claude/local/messages"
cat > "$TMPDIR/t6/.claude/local/messages/health.json" <<EOF
{
  "daemon": "test", "version": "1.0", "pid": 1,
  "started_at": "$NOW",
  "last_cycle": "$FIVE_M",
  "cycle_count": 42, "cycle_duration_ms": 20,
  "adapters": {
    "signal": {
      "platform": "signal", "tier": 0,
      "last_success": "$NOW",
      "last_failure": null, "last_error": null,
      "last_duration_ms": 16,
      "last_yield": {"messages":0,"threads":0,"contacts":0},
      "consecutive_failures": 0, "timed_out": false,
      "source_observed_at": "$ONE_H",
      "source_evidence": "signal-desktop authenticated websocket keepalive",
      "skipped": false, "cooldown_until": null, "skip_reason": null
    }
  }
}
EOF
check "$TMPDIR/t6" 1 "fresh empty poll cannot hide stale Signal source"

echo ""
echo "=== SUMMARY ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
echo "  ALL TESTS PASSED"
