---
name: storybook
description: Generate D3 force-directed knowledge graph from message entities
---

**Phase 4 — Not yet implemented.**

This command will:
1. Run entity extraction on unprocessed message threads
2. Query FalkorDB for message: namespace entities and relationships
3. Generate an interactive D3 force-directed graph HTML
4. Open it in the browser with `xdg-open`

To prepare for this, ensure:
- FalkorDB is running: `systemctl --user status hippo-graph`
- Entity extraction script exists: `scripts/extract_entities.py`
