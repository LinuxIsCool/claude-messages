"""Pytest fixtures for the claude-messages webui.

Builds a tiny SQLite DB with the real messages.db schema subset (messages +
threads + contacts + identities + identity_links + contact_scores +
messages_fts) so the data layer is exercised against real SQL — no mocks.
Includes a SCORED sender (Darren) and an UNSCORED sender (raw handle) to
exercise graceful degradation.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

# Make web/ importable for all tests.
WEB_DIR = Path(__file__).resolve().parent.parent / "web"
if str(WEB_DIR) not in sys.path:
    sys.path.insert(0, str(WEB_DIR))


def _build_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE messages (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, thread_id TEXT,
            sender_id TEXT, content TEXT, content_type TEXT DEFAULT 'text',
            reply_to TEXT, metadata TEXT DEFAULT '{}', platform_ts TEXT NOT NULL,
            synced_at TEXT NOT NULL, direction TEXT DEFAULT 'unknown'
        );
        CREATE TABLE threads (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, title TEXT,
            thread_type TEXT, participants TEXT DEFAULT '[]',
            metadata TEXT DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE contacts (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, display_name TEXT,
            username TEXT, phone TEXT, metadata TEXT DEFAULT '{}',
            first_seen TEXT NOT NULL, last_seen TEXT NOT NULL
        );
        CREATE TABLE identities (
            id TEXT PRIMARY KEY, display_name TEXT NOT NULL, notes TEXT,
            metadata TEXT DEFAULT '{}', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE identity_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT, identity_id TEXT NOT NULL,
            platform TEXT NOT NULL, platform_id TEXT NOT NULL, display_name TEXT,
            username TEXT, confidence REAL DEFAULT 1.0, source TEXT DEFAULT 'manual',
            metadata TEXT DEFAULT '{}', created_at TEXT NOT NULL,
            UNIQUE(platform, platform_id)
        );
        CREATE TABLE contact_scores (
            identity_id TEXT PRIMARY KEY, frequency REAL DEFAULT 0, recency REAL DEFAULT 0,
            reciprocity REAL DEFAULT 0, channel_diversity REAL DEFAULT 0, dm_ratio REAL DEFAULT 0,
            structural REAL DEFAULT 0, temporal_regularity REAL DEFAULT 0, response_latency REAL DEFAULT 0,
            composite REAL DEFAULT 0, dunbar_layer TEXT DEFAULT 'acquaintance',
            confidence REAL DEFAULT 0, computed_at TEXT NOT NULL DEFAULT ''
        );
        CREATE VIRTUAL TABLE messages_fts USING fts5(content);
        CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;
        """
    )
    # Threads
    conn.execute(
        "INSERT INTO threads VALUES ('t1','telegram','Darren ↔ Shawn','dm','[]','{}','2026-05-01T00:00:00Z','2026-06-01T10:00:00Z')"
    )
    conn.execute(
        "INSERT INTO threads VALUES ('t2','signal','Unknown group','group','[]','{}','2026-05-01T00:00:00Z','2026-05-20T09:00:00Z')"
    )
    # Identity + link + score for Darren (SCORED)
    conn.execute(
        "INSERT INTO identities VALUES ('id-darren','Darren Zal',NULL,'{}','2026-03-11T00:00:00Z','2026-03-11T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO identity_links(identity_id,platform,platform_id,display_name,created_at) "
        "VALUES ('id-darren','telegram','user:458825601','Darren Zal','2026-03-11T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO contact_scores(identity_id,composite,dunbar_layer,confidence,computed_at) "
        "VALUES ('id-darren',0.854,'support_clique',0.9,'2026-05-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO contacts VALUES ('telegram:user:458825601','telegram','Darren Zal','Darren432',NULL,'{}','2026-03-11T00:00:00Z','2026-06-01T00:00:00Z')"
    )
    # Messages — newest last in insert order; ids m1(oldest)..m4(newest)
    rows = [
        ("m1", "telegram", "t1", "telegram:user:458825601", "early multisig note", "2026-05-01T08:00:00Z", "received"),
        ("m2", "telegram", "t1", "telegram:user:458825601", "lets align on the multisig split", "2026-06-01T10:00:00Z", "received"),
        ("m3", "signal", "t2", "signal:user:zzz-unknown", "random unscored chatter", "2026-05-20T09:00:00Z", "received"),
        ("m4", "telegram", "t1", "telegram:user:000-me", "ok sounds good", "2026-06-01T11:00:00Z", "sent"),
    ]
    for mid, plat, tid, sid, content, ts, direction in rows:
        conn.execute(
            "INSERT INTO messages(id,platform,thread_id,sender_id,content,platform_ts,synced_at,direction) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (mid, plat, tid, sid, content, ts, ts, direction),
        )
    conn.commit()
    conn.close()


@pytest.fixture()
def fixture_db(tmp_path: Path) -> Path:
    """Path to a freshly-built fixture messages.db."""
    db = tmp_path / "messages.db"
    _build_db(db)
    return db
