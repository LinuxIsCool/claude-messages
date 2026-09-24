# claude-messages

Unified messaging backbone. Syncs Telegram, Signal, Email, and Slack into SQLite with FTS5 search.

> **Rendering contract (CANONICAL):** Any agent displaying messages from this plugin MUST use the header-block + table format (Platform · Thread · Group Members · Summary header; `marker | Author | Day | Timestamp-with-year | full untruncated Message` rows; ascending order; `...` gap rows; ≥1 context buffer each side). Spec: `~/.claude/projects/-home-shawn/memory/message-rendering-contract.md`. (Shawn directive 2026-06-16; see backlog task-4151.)

## Quick Start
- `/messages <query>` — search messages
- `/messages-status` — daemon + sync health
- `/storybook` — generate D3 knowledge graph (Phase 4)

## MCP Tools
| Tool | Description |
|------|-------------|
| `search_messages` | Full-text search — text/json/compact output, dm_only filter, smart suggestions |
| `recent_messages` | Latest messages — text/json/compact output, dm_only filter |
| `get_thread` | Thread messages with sender names — text/json/compact output |
| `list_threads` | List threads — text/json/compact output, cached summaries |
| `thread_messages` | **Temporal thread navigation** — around/after/before with natural dates |
| `messages_timeframe` | **Global time-windowed search** — person/query filter, natural dates |
| `get_thread_summary` | **LLM thread summary** — cached, auto-regenerates when stale |
| `message_timeline` | **Activity visualization** — ASCII bar chart by month |
| `messages_person` | **One-call person search** — resolves identity + searches, DM-first |
| `get_message_context` | Context window around a message (N before + N after) |
| `message_stats` | Counts by platform, date range |
| `resolve_contact` | Look up unified identity for a platform contact |
| `who_is` | Fuzzy search across names, phones, usernames — returns identity cards |
| `link_identities` | Link a contact to an identity (creates identity if needed) |
| `unlink_identity` | Remove a platform link from an identity |
| `merge_identities` | Merge two identities (source absorbed into target) |
| `list_identities` | Browse/search unified identities |
| `get_identity` | Full identity card with links, stats, events |
| `unlinked_contacts` | Audit unlinked contacts sorted by message activity |
| `auto_resolve` | Cross-platform identity matching (phone + email + single-platform + name) |
| `identity_health` | Diagnostic view of identity resolution coverage |
| `update_identity` | Update an identity's display name or notes |
| `cleanup_identities` | Remove orphaned identities with zero links |
| `identity_relationships` | Who talks to whom — shared thread participation |
| `merge_suggestions` | Surface ambiguous name matches for human review |
| `export_identities` | Bulk export all identities for plugin integration |
| `relationship_score` | Full ContactRank score breakdown for one contact (8 factors + composite) |
| `inner_circle` | Contacts ranked by relationship strength, grouped by Dunbar layer |
| `fading_relationships` | Detect unusually silent contacts — inner circle first |
| `refresh_scores` | Recompute all ContactRank scores (stores self_identity_id on first call) |
| `set_dunbar_override` | Manual Dunbar layer override — survives re-scoring |

## Infrastructure
- Daemon: `systemctl --user {start,stop,status} legion-messages`
- DB: `~/.claude/local/messages/messages.db` (SQLite + FTS5)
- Events: `~/.claude/local/messages/events/` (JSONL audit trail)
- Logs: `~/.claude/local/messages/logs/daemon.log`
- Config: `~/.claude/local/messages/config.yml`
- Secrets: `~/.claude/local/messages/secrets/` (gitignored)
- Research: `~/.claude/local/research/messages/` (deep research reports, roadmap)

## Data Schema

### SQLite: `~/.claude/local/messages/messages.db`

```sql
CREATE TABLE contacts (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    display_name TEXT,
    username TEXT,
    phone TEXT,
    metadata TEXT DEFAULT '{}',
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL
);

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    title TEXT,
    thread_type TEXT,
    participants TEXT DEFAULT '[]',
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    thread_id TEXT,
    sender_id TEXT,
    content TEXT,
    content_type TEXT DEFAULT 'text',
    reply_to TEXT,
    metadata TEXT DEFAULT '{}',
    platform_ts TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    direction TEXT DEFAULT 'unknown'
);

CREATE TABLE sync_cursors (
    adapter TEXT PRIMARY KEY,
    cursor_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE extraction_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT,
    message_range TEXT,
    status TEXT DEFAULT 'pending',
    triples_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE identities (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    notes TEXT,
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE identity_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_id TEXT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    platform_id TEXT NOT NULL,
    display_name TEXT,
    username TEXT,
    confidence REAL DEFAULT 1.0,
    source TEXT DEFAULT 'manual',
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(platform, platform_id)
);

CREATE TABLE identity_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    identity_id TEXT NOT NULL,
    details TEXT DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE nickname_map (
    canonical TEXT NOT NULL,
    nickname TEXT NOT NULL,
    PRIMARY KEY (canonical, nickname)
);

CREATE TABLE backfill_state (
    dialog_id TEXT PRIMARY KEY,
    platform TEXT NOT NULL DEFAULT 'telegram',
    dialog_title TEXT,
    dialog_type TEXT,
    messages_fetched INTEGER DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    status TEXT DEFAULT 'pending'
);

CREATE TABLE contact_scores (
    identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
    frequency REAL DEFAULT 0,
    recency REAL DEFAULT 0,
    reciprocity REAL DEFAULT 0,
    channel_diversity REAL DEFAULT 0,
    dm_ratio REAL DEFAULT 0,
    structural REAL DEFAULT 0,
    temporal_regularity REAL DEFAULT 0,
    response_latency REAL DEFAULT 0,
    composite REAL DEFAULT 0,
    dunbar_layer TEXT DEFAULT 'acquaintance',
    confidence REAL DEFAULT 0,
    computed_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE tier_overrides (
    identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
    dunbar_layer TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE thread_summaries (
    thread_id TEXT PRIMARY KEY REFERENCES threads(id),
    summary TEXT NOT NULL,
    message_count INTEGER NOT NULL,
    last_message_ts TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT 'telus-gpt-oss'
);
```

### FTS5

```sql
CREATE VIRTUAL TABLE messages_fts USING fts5(content, tokenize='porter unicode61');
-- Auto-maintained via INSERT/UPDATE/DELETE triggers on messages table
```

### File Layout

```
~/.claude/local/messages/
  messages.db                      # SQLite (15 tables + FTS5 + triggers)
  config.yml                       # Daemon configuration
  events/
    YYYY-MM.jsonl                  # Monthly audit trail (append-only)
  logs/
    daemon.log                     # Daemon output
  secrets/                         # API keys (gitignored)
```

### JSONL: `events/YYYY-MM.jsonl`

```json
{
  "type": "contact.upsert",
  "data": {
    "id": "telegram:user:1620491206",
    "platform": "telegram",
    "display_name": "User 1620491206",
    "metadata": {"bot": false},
    "first_seen": "2026-04-01T06:53:01.741Z",
    "last_seen": "2026-04-01T06:53:01.741Z"
  },
  "ts": "2026-04-01T07:00:00.106Z"
}
```

### Canonical Counts

```sql
SELECT platform, COUNT(*) FROM messages GROUP BY platform;
SELECT COUNT(*) FROM threads;
SELECT COUNT(*) FROM identities;
```

## Development
- Build: `cd server && npm run build` (esbuild → daemon.mjs + mcp.mjs)
- Test: `cd server && npm test` (vitest — 23 tests)
- Test MCP: `timeout 3 node server/build/mcp.mjs 2>&1 || true`
- Rebuild: `cd server && npm install && npm run build`
