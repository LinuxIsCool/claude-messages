---
name: messages
description: Unified message search, sync management, and knowledge extraction across platforms
---

# Messages Skill

Search and manage synced messages from Telegram, Signal, and Email.

## Display Format (MANDATORY)

When presenting messages to the user, ALWAYS include ALL of these fields:

| Field | Description |
|-------|-------------|
| **Platform** | Signal, Telegram, Email, Slack |
| **Thread** | Thread name or ID |
| **Participants** | All participants in the thread |
| **Timestamp** | Full ISO 8601 or human-readable datetime |
| **Author** | Who sent this specific message |
| **Exact Message** | Full verbatim text — NEVER paraphrase or summarize |

Example:
> **Platform**: Signal | **Thread**: Regen <> Gaia | **Participants**: Darren, Shawn, Dave, Samu
> **Author**: Darren | **Timestamp**: 2026-04-02T06:00:15Z
> **Message**: "I scheduled a demo with seatrees for friday at 12:30-1, will you be able to make that?"

This applies to all contexts: briefs, digests, search results, thread views, and any time a message is cited in conversation.

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
