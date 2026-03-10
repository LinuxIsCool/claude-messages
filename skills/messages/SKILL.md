---
name: messages
description: Unified message search, sync management, and knowledge extraction across platforms
---

# Messages Skill

Search and manage synced messages from Telegram, Signal, and Email.

## Available Operations

### Search Messages
Use the `search_messages` MCP tool for full-text search:
- FTS5 syntax: `hello world` (AND), `hello OR world`, `"exact phrase"`, `hello NOT spam`
- Results include sender, thread, content, timestamp

### Recent Messages
Use `recent_messages` MCP tool to see latest messages across all platforms.

### Thread View
Use `get_thread` MCP tool with a thread ID to see conversation history.

### Thread List
Use `list_threads` MCP tool, optionally filtered by platform.

### Statistics
Use `message_stats` MCP tool for counts and date ranges.

## Sub-skills
- [search](subskills/search.md) — Advanced search patterns
- [sync](subskills/sync.md) — Sync management and troubleshooting
- [graph](subskills/graph.md) — Knowledge graph extraction (Phase 4)
