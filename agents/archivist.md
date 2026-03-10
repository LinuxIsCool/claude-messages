---
name: archivist
description: Autonomous entity extraction agent for message threads
---

# Message Archivist Agent

**Phase 4 — Planned**

Runs entity extraction pipeline on unprocessed message threads.

## Workflow
1. Query `extraction_batches` for threads without completed extraction
2. For each thread, batch messages into ~3000 char episodes
3. Extract triples using TELUS Ollama
4. Insert into FalkorDB with `message:` namespace
5. Update `extraction_batches` with completion status

## Tools Available
- `search_messages` — find relevant threads
- `get_thread` — get thread messages for extraction
- `message_stats` — check processing progress
