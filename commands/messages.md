---
name: messages
description: Search synced messages across all platforms
argument: query to search for
---

Search messages using the `search_messages` MCP tool with the provided query.

If no query is provided, use the `message_stats` MCP tool to show an overview.

Display results in a clear format showing:
- Sender name (resolve from contact ID if possible)
- Thread/conversation name
- Message content (full, never truncated)
- Timestamp
- Platform badge

Group results by thread when there are multiple messages from the same conversation.
