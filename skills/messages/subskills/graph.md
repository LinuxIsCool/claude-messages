---
name: graph
description: Knowledge graph extraction from messages (Phase 4)
---

# Message Knowledge Graph

**Phase 4 — Planned**

Will extract entities and relationships from message threads into FalkorDB
using the `message:` namespace, compatible with the hippo plugin's graph.

## Planned Entity Types
Person, Organization, Project, Topic, Event, Location

## Planned Workflow
1. Batch messages by thread into episodes (~3000 char windows)
2. Extract triples via TELUS Ollama (gpt-oss:120b)
3. Insert into FalkorDB with `message:` namespace prefix
4. Cross-reference with journal:, venture:, and other namespaces
