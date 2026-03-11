# claude-messages

Unified messaging backbone. Syncs Telegram (Signal, Email coming) into SQLite with FTS5 search.

## Quick Start
- `/messages <query>` — search messages
- `/messages-status` — daemon + sync health
- `/storybook` — generate D3 knowledge graph (Phase 4)

## MCP Tools
| Tool | Description |
|------|-------------|
| `search_messages` | Full-text search across all synced messages |
| `recent_messages` | Get latest messages across platforms |
| `get_thread` | Get messages from a specific thread |
| `list_threads` | List conversation threads |
| `message_stats` | Counts by platform, date range |
| `resolve_contact` | Look up unified identity for a platform contact |
| `who_is` | Fuzzy search across names, phones, usernames — returns identity cards |
| `link_identities` | Link a contact to an identity (creates identity if needed) |
| `unlink_identity` | Remove a platform link from an identity |
| `merge_identities` | Merge two identities (source absorbed into target) |
| `list_identities` | Browse/search unified identities |
| `get_identity` | Full identity card with links, stats, events |
| `auto_resolve` | Phone-based cross-platform identity matching |

## Infrastructure
- Daemon: `systemctl --user {start,stop,status} legion-messages`
- DB: `~/.claude/local/messages/messages.db` (SQLite + FTS5)
- Events: `~/.claude/local/messages/events/` (JSONL audit trail)
- Logs: `~/.claude/local/messages/logs/daemon.log`
- Config: `~/.claude/local/messages/config.yml`
- Secrets: `~/.claude/local/messages/secrets/` (gitignored)

## Development
- Build: `cd server && npm run build` (esbuild → daemon.mjs + mcp.mjs)
- Test MCP: `timeout 3 node server/build/mcp.mjs 2>&1 || true`
- Rebuild: `cd server && npm install && npm run build`
