---
name: search
description: Advanced message search patterns
---

# Message Search

## FTS5 Query Syntax
- Simple: `hello` — matches messages containing "hello"
- Phrase: `"machine learning"` — exact phrase match
- AND: `hello world` — both terms (default)
- OR: `hello OR hi` — either term
- NOT: `hello NOT spam` — exclude term
- Prefix: `mach*` — prefix match
- Near: `NEAR(hello world, 5)` — within 5 tokens

## Search Strategies
1. Start broad, narrow with platform/thread filters
2. Use `list_threads` to find relevant thread IDs, then `get_thread` for context
3. For time-bounded queries, sort results by timestamp after retrieval

## Semantic Search (Phase 3)
When `semantic_search` MCP tool is available, use it for meaning-based queries
that don't depend on exact keyword matches.
