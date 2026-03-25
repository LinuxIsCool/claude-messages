---
name: messages-ddl
description: Complete database schema (DDL) for the messages plugin — SQLite messages.db and KOI PostgreSQL bundle storage. Load this when writing queries, building graphs, or analyzing message data.
---

# Messages Database Schema

## SQLite: `~/.claude/local/messages/messages.db`

Unified messaging backbone. WAL mode, foreign keys ON, FTS5 auto-sync.

### Core Tables

```sql
CREATE TABLE contacts (
    id TEXT PRIMARY KEY,          -- "telegram:user:1441369482"
    platform TEXT NOT NULL,       -- telegram, signal, email, slack
    display_name TEXT,
    username TEXT,
    phone TEXT,
    metadata TEXT DEFAULT '{}',   -- JSON: avatar_url, bio, etc.
    first_seen TEXT NOT NULL,     -- ISO 8601
    last_seen TEXT NOT NULL
);

CREATE TABLE threads (
    id TEXT PRIMARY KEY,          -- "telegram:chat:-1001968127505"
    platform TEXT NOT NULL,
    title TEXT,                   -- Group name or DM partner name
    thread_type TEXT,             -- dm, private, group, channel, supergroup
    participants TEXT DEFAULT '[]', -- JSON array of contact IDs
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,          -- "telegram:msg:12345"
    platform TEXT NOT NULL,
    thread_id TEXT,               -- FK to threads.id (not enforced)
    sender_id TEXT,               -- FK to contacts.id (not enforced)
    content TEXT,                 -- Message body (plain text)
    content_type TEXT DEFAULT 'text', -- text, photo, document, sticker, etc.
    reply_to TEXT,                -- Message ID being replied to
    metadata TEXT DEFAULT '{}',   -- JSON: forward_from, edit_date, etc.
    platform_ts TEXT NOT NULL,    -- Original platform timestamp (ISO 8601)
    synced_at TEXT NOT NULL       -- When we synced it
);

CREATE TABLE sync_cursors (
    adapter TEXT PRIMARY KEY,     -- "telegram", "signal", etc.
    cursor_value TEXT NOT NULL,   -- Adapter-specific sync position
    updated_at TEXT NOT NULL
);
```

### Identity Resolution

```sql
CREATE TABLE identities (
    id TEXT PRIMARY KEY,          -- UUID
    display_name TEXT NOT NULL,   -- Canonical name: "Darren Zal"
    notes TEXT,
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE identity_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identity_id TEXT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,       -- telegram, signal, email
    platform_id TEXT NOT NULL,    -- "telegram:user:1441369482"
    display_name TEXT,            -- Platform-specific display name
    username TEXT,
    confidence REAL DEFAULT 1.0,  -- 0.0-1.0, manual=1.0
    source TEXT DEFAULT 'manual', -- manual, auto_phone, auto_email, auto_name
    metadata TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE(platform, platform_id)
);

CREATE TABLE identity_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,     -- link_created, link_removed, merge, etc.
    identity_id TEXT NOT NULL,
    details TEXT DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE nickname_map (
    canonical TEXT NOT NULL,      -- "Darren Zal"
    nickname TEXT NOT NULL,       -- "darren", "DZ", "zal"
    PRIMARY KEY (canonical, nickname)
);
```

### Relationship Scoring (ContactRank)

```sql
CREATE TABLE contact_scores (
    identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
    frequency REAL DEFAULT 0,           -- Message volume (normalized)
    recency REAL DEFAULT 0,             -- How recently they messaged
    reciprocity REAL DEFAULT 0,         -- Send/receive balance
    channel_diversity REAL DEFAULT 0,   -- How many platforms
    dm_ratio REAL DEFAULT 0,            -- DM vs group ratio
    structural REAL DEFAULT 0,          -- Shared group membership
    temporal_regularity REAL DEFAULT 0, -- Consistency of communication
    response_latency REAL DEFAULT 0,    -- How fast they respond
    composite REAL DEFAULT 0,           -- Weighted aggregate score
    dunbar_layer TEXT DEFAULT 'acquaintance', -- intimate/close/active/acquaintance
    confidence REAL DEFAULT 0,
    computed_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE tier_overrides (
    identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
    dunbar_layer TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);
```

### Full-Text Search

```sql
CREATE VIRTUAL TABLE messages_fts USING fts5(
    content,
    content=messages,
    content_rowid=rowid,
    tokenize='porter unicode61'
);

-- Auto-sync triggers (INSERT, UPDATE, DELETE)
CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
    INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
```

### Other Tables

```sql
CREATE TABLE extraction_batches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT,
    message_range TEXT,
    status TEXT DEFAULT 'pending',  -- pending, processing, complete, error
    triples_count INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    completed_at TEXT
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

CREATE TABLE config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

### Indexes

```sql
CREATE INDEX idx_messages_thread ON messages(thread_id);
CREATE INDEX idx_messages_platform ON messages(platform);
CREATE INDEX idx_messages_sender ON messages(sender_id);
CREATE INDEX idx_messages_ts ON messages(platform_ts);
CREATE INDEX idx_contacts_platform ON contacts(platform);
CREATE INDEX idx_threads_platform ON threads(platform);
CREATE INDEX idx_identity_links_identity ON identity_links(identity_id);
CREATE INDEX idx_identity_links_platform ON identity_links(platform, platform_id);
CREATE INDEX idx_identity_events_identity ON identity_events(identity_id);
CREATE INDEX idx_nickname_nickname ON nickname_map(nickname);
```

## PostgreSQL: KOI Bundle Storage (`personal_koi`)

Message bundles in KOI-net with vector embeddings and entity extraction.

### Bundle Table

```sql
CREATE TABLE bundles (
    rid TEXT PRIMARY KEY,           -- "orn:legion.claude-message:telegram:msg:12345"
    namespace TEXT NOT NULL,        -- "legion.claude-message"
    reference TEXT NOT NULL,        -- "telegram:msg:12345"
    contents JSONB NOT NULL,        -- Full message data + _koi_tier
    search_text TEXT NOT NULL DEFAULT '',
    sha256_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('english', search_text)
    ) STORED
);
```

Message bundle `contents` JSONB shape:
```json
{
    "message_id": "telegram:msg:12345",
    "platform": "telegram",
    "thread_id": "telegram:chat:-1001968127505",
    "sender_id": "telegram:user:1441369482",
    "content": "message text here",
    "content_type": "text",
    "platform_ts": "2024-06-15T10:30:00Z",
    "_koi_tier": "active"
}
```

### Entity Extraction

```sql
CREATE TABLE entities (
    entity_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    entity_type TEXT NOT NULL,       -- Person, Organization, Topic, Tool, etc.
    supertype TEXT NOT NULL DEFAULT '', -- agent, artifact, concept, event, place
    name_normalized TEXT NOT NULL,
    description TEXT,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mention_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(name_normalized, entity_type)
);

CREATE TABLE bundle_entities (
    rid TEXT NOT NULL REFERENCES bundles(rid) ON DELETE CASCADE,
    entity_id INTEGER NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    mention_count INTEGER NOT NULL DEFAULT 1,
    confidence FLOAT NOT NULL DEFAULT 1.0,
    PRIMARY KEY (rid, entity_id)
);

CREATE TABLE relations (
    relation_id SERIAL PRIMARY KEY,
    source_entity_id INTEGER NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    target_entity_id INTEGER NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL,      -- works_with, discusses, member_of, etc.
    confidence FLOAT NOT NULL DEFAULT 1.0,
    evidence TEXT DEFAULT '',
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mention_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(source_entity_id, target_entity_id, relation_type)
);
```

### Embedding Tables (per config)

```sql
-- Template: embeddings_{config_id}
CREATE TABLE embeddings_{config} (
    rid TEXT NOT NULL REFERENCES bundles(rid) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    chunk_text TEXT,
    embedding vector({dim}),     -- pgvector
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rid, chunk_index)
);
```

Active configs: `telus_e5_1024` (1024d), `ollama_nomic_768` (768d), `ollama_nomic_768_ctx` (768d contextual), `ollama_mxbai_1024` (1024d).

## Data Scale (as of 2026-03-14)

| Metric | Count |
|--------|-------|
| Messages (SQLite) | 850,607 |
| Threads | 29,978 |
| Contacts | 58,000+ |
| Identities (resolved) | ~500 |
| KOI bundles (messages) | 404,229 |
| KOI entities | 11,848 |
| Embeddings (nomic-ctx) | 594,090 |

## Common Query Patterns

```sql
-- Messages by identity (cross-platform)
SELECT m.* FROM messages m
JOIN identity_links il ON m.sender_id = il.platform_id
WHERE il.identity_id = ?
ORDER BY m.platform_ts DESC;

-- Thread activity with sender names
SELECT m.*, c.display_name as sender_name
FROM messages m
LEFT JOIN contacts c ON m.sender_id = c.id
WHERE m.thread_id = ?
ORDER BY m.platform_ts;

-- Co-occurrence: who appears in same threads
SELECT il1.identity_id as person_a, il2.identity_id as person_b,
       COUNT(DISTINCT m1.thread_id) as shared_threads
FROM messages m1
JOIN identity_links il1 ON m1.sender_id = il1.platform_id
JOIN messages m2 ON m1.thread_id = m2.thread_id AND m1.sender_id != m2.sender_id
JOIN identity_links il2 ON m2.sender_id = il2.platform_id
WHERE il1.identity_id < il2.identity_id
GROUP BY il1.identity_id, il2.identity_id
ORDER BY shared_threads DESC;

-- FTS search
SELECT m.*, t.title as thread_title
FROM messages m
JOIN messages_fts ON messages_fts.rowid = m.rowid
JOIN threads t ON m.thread_id = t.id
WHERE messages_fts MATCH ?
ORDER BY rank;
```
