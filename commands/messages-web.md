---
description: Launch the claude-messages webui at http://127.0.0.1:8840/ (read-only)
allowed-tools: ["Bash"]
---

# /messages-web

Launches the claude-messages webui at `http://127.0.0.1:8840/`.

Kernel-pattern Python HTTP server (stdlib + claude_webui — no framework) that
renders the 886K-message corpus as reverse-chronological cards with platform
glyph + identicon, identity/relationship-score join, FTS5 search, and
platform/sender/date/direction filters. Click any card for a detail view.
New messages appear live via SSE.

**Read-only**: `messages.db` opened `mode=ro` + `PRAGMA query_only`; all non-GET
verbs return 405. 127.0.0.1 default bind — `--bind 0.0.0.0` to expose over Tailscale.

**Phase 1** (task-556): Messages surface only. Threads page = Phase 2; derived
tags/ventures = Phase 3; saved views = Phase 4.

```bash
cd "${CLAUDE_PLUGIN_ROOT}/web" && uv run python server.py
```
