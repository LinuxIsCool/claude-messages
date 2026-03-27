# claude-messages

Unified messaging backbone. Syncs Telegram, Signal, Email, and Slack into SQLite with FTS5 search.

## Quick Start
- `/messages <query>` — search messages
- `/messages-status` — daemon + sync health
- `/storybook` — generate D3 knowledge graph (Phase 4)

## MCP Tools
| Tool | Description |
|------|-------------|
| `search_messages` | Full-text search — text/json/compact output, dm_only filter |
| `recent_messages` | Latest messages — text/json/compact output, dm_only filter |
| `get_thread` | Thread messages with sender names — text/json/compact output |
| `list_threads` | List threads — text/json/compact output |
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

## Development
- Build: `cd server && npm run build` (esbuild → daemon.mjs + mcp.mjs)
- Test: `cd server && npm test` (vitest — 23 tests)
- Test MCP: `timeout 3 node server/build/mcp.mjs 2>&1 || true`
- Rebuild: `cd server && npm install && npm run build`
