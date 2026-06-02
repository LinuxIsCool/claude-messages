"""claude-messages webui — read-only data layer.

All SQL against the canonical messages.db lives here. The DB is opened
read-only (mode=ro + PRAGMA query_only) — this module NEVER writes. Higher
layers (accessor/handler) do arg coercion and HTTP; this module only knows
the schema.

Join keys (verified 2026-06-02):
  - messages.sender_id is platform-prefixed: "telegram:user:458825601".
  - contacts.id uses the same prefixed format.
  - identity_links.platform_id is UNPREFIXED ("user:458825601"), so the join
    strips the "<platform>:" prefix via substr(sender_id, length(platform)+2).
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

DEFAULT_DB_PATH = Path(
    os.environ.get(
        "MESSAGES_DB_PATH",
        str(Path.home() / ".claude" / "local" / "messages" / "messages.db"),
    )
)

DEFAULT_LIMIT = 60
MAX_LIMIT = 200

# Column projection shared by list + search. `m`,`t`,`il`,`idn`,`c`,`cs` are
# bound by _LEFT_JOINS below.
_SELECT_COLS = """
SELECT m.id, m.platform, m.thread_id, m.sender_id, m.content, m.content_type,
       m.reply_to, m.platform_ts AS timestamp, m.direction,
       t.title AS thread_title, t.participants AS thread_participants,
       il.identity_id AS sender_identity_id,
       COALESCE(idn.display_name, c.display_name, m.sender_id) AS sender_name,
       cs.composite AS connectedness, cs.dunbar_layer AS priority_tier
"""

_LEFT_JOINS = """
LEFT JOIN threads t        ON t.id = m.thread_id
LEFT JOIN identity_links il ON il.platform = m.platform
                           AND il.platform_id = substr(m.sender_id, length(m.platform) + 2)
LEFT JOIN identities idn   ON idn.id = il.identity_id
LEFT JOIN contacts c       ON c.id = m.sender_id
LEFT JOIN contact_scores cs ON cs.identity_id = il.identity_id
"""


def connect_ro(db_path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Open the messages DB strictly read-only."""
    uri = f"file:{Path(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = 1")
    return conn


def _clamp_limit(params: dict[str, Any]) -> int:
    try:
        n = int(params.get("limit", DEFAULT_LIMIT))
    except (TypeError, ValueError):
        n = DEFAULT_LIMIT
    return max(1, min(n, MAX_LIMIT))


def _offset(params: dict[str, Any]) -> int:
    try:
        return max(0, int(params.get("offset", 0)))
    except (TypeError, ValueError):
        return 0


def _filters(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build the shared WHERE fragment (excluding any FTS MATCH) + binds."""
    clauses: list[str] = []
    binds: dict[str, Any] = {}
    if params.get("platform"):
        clauses.append("AND m.platform = :platform")
        binds["platform"] = params["platform"]
    if params.get("sender"):
        clauses.append("AND m.sender_id = :sender")
        binds["sender"] = params["sender"]
    if params.get("since"):
        clauses.append("AND m.platform_ts >= :since")
        binds["since"] = params["since"]
    if params.get("until"):
        clauses.append("AND m.platform_ts <= :until")
        binds["until"] = params["until"]
    if params.get("direction"):
        clauses.append("AND m.direction = :direction")
        binds["direction"] = params["direction"]
    return " ".join(clauses), binds


def _row_to_card(row: sqlite3.Row) -> dict[str, Any]:
    """Shape a DB row into the card contract. tags/ventures are Phase-3 — empty here."""
    return {
        "id": row["id"],
        "platform": row["platform"],
        "thread_id": row["thread_id"],
        "thread_title": row["thread_title"],
        "sender_id": row["sender_id"],
        "sender_name": row["sender_name"],
        "content": row["content"],
        "content_type": row["content_type"],
        "reply_to": row["reply_to"],
        "timestamp": row["timestamp"],
        "direction": row["direction"],
        "connectedness": row["connectedness"],
        "priority_tier": row["priority_tier"],
        "tags": [],
        "ventures": [],
    }


def list_messages(conn: sqlite3.Connection, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Reverse-chronological message list with optional filters + pagination."""
    filt, binds = _filters(params)
    binds["limit"] = _clamp_limit(params)
    binds["offset"] = _offset(params)
    sql = (
        f"{_SELECT_COLS} FROM messages m {_LEFT_JOINS} "
        f"WHERE 1=1 {filt} "
        f"ORDER BY m.platform_ts DESC LIMIT :limit OFFSET :offset"
    )
    return [_row_to_card(r) for r in conn.execute(sql, binds).fetchall()]
