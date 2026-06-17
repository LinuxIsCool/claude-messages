import sqlite3, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import messages_data as md

def _db():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
      CREATE TABLE threads(id TEXT PRIMARY KEY, platform TEXT, title TEXT, thread_type TEXT,
        participants TEXT, metadata TEXT, created_at TEXT, updated_at TEXT);
      CREATE TABLE messages(id TEXT PRIMARY KEY, platform TEXT, thread_id TEXT, sender_id TEXT,
        content TEXT, content_type TEXT, reply_to TEXT, metadata TEXT, platform_ts TEXT,
        synced_at TEXT, direction TEXT);
      CREATE TABLE identities(id TEXT PRIMARY KEY, display_name TEXT, notes TEXT, metadata TEXT, created_at TEXT, updated_at TEXT);
      CREATE TABLE identity_links(id INTEGER PRIMARY KEY, identity_id TEXT, platform TEXT, platform_id TEXT, display_name TEXT, username TEXT, confidence REAL, source TEXT, metadata TEXT, created_at TEXT);
      CREATE TABLE contact_scores(identity_id TEXT PRIMARY KEY, composite REAL, dunbar_layer TEXT, computed_at TEXT);
    """)
    c.execute("INSERT INTO threads VALUES('t1','signal','Darren chat','dm','[]','{}','x','2026-06-13T10:00:00Z')")
    c.execute("INSERT INTO threads VALUES('t2','email','Invoice','dm','[]','{}','x','2026-06-12T10:00:00Z')")
    c.execute("INSERT INTO identities VALUES('i1','Darren Zal',NULL,'{}','x','x')")
    c.execute("INSERT INTO identity_links VALUES(1,'i1','signal','u-d','Darren',NULL,1.0,'manual','{}','x')")
    c.execute("INSERT INTO contact_scores VALUES('i1',0.85,'support_clique','x')")
    c.execute("INSERT INTO messages VALUES('m1','signal','t1','signal:u-d','hi','text',NULL,'{}','2026-06-13T10:00:00Z','x','received')")
    c.execute("INSERT INTO messages VALUES('m2','email','t2','email:me','sent it','text',NULL,'{}','2026-06-12T10:00:00Z','x','sent')")
    return c

def test_list_threads_needs_reply_and_platforms():
    c = _db()
    by = {r["id"]: r for r in md.list_threads(c, {"limit": 50})}
    assert by["t1"]["needs_reply"] is True
    assert by["t2"]["needs_reply"] is False
    only_email = md.list_threads(c, {"limit": 50, "platforms": "email"})
    assert {r["id"] for r in only_email} == {"t2"}

def test_list_threads_participants():
    c = _db()
    by = {r["id"]: r for r in md.list_threads(c, {"limit": 50})}
    parts = by["t1"]["participants"]
    assert isinstance(parts, list)
    assert parts and parts[0]["name"] == "Darren Zal" and parts[0]["dunbar_layer"] == "support_clique"

def test_list_threads_sort_last_activity():
    c = _db()
    rec = md.list_threads(c, {"limit": 50, "sort": "last_activity"})
    assert [r["id"] for r in rec] == ["t1", "t2"]
