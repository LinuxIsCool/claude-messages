"""MessagesAccessor — Accessor Protocol over the messages.db read-only data layer.

Opens one read-only connection per accessor instance (shared across requests;
sqlite3 with check_same_thread=False + query_only is safe for concurrent
reads). Arg coercion happens here; SQL lives in messages_data.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import messages_data as md
from claude_webui.healthz import healthz_response

__version__ = "0.1.0"

NAMESPACE = "legion.claude-message"


class MessagesAccessor:
    """Substrate accessor for the claude-messages webui (Phase 1: read-only)."""

    def __init__(self, db_path: Path | str = md.DEFAULT_DB_PATH) -> None:
        self._db_path = Path(db_path)
        self._conn = md.connect_ro(self._db_path)

    # ── Accessor Protocol ────────────────────────────────────────────
    def list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        return md.list_messages(self._conn, dict(params))

    def detail(self, item_id: str) -> dict[str, Any]:
        rec = md.get_message_detail(self._conn, item_id)
        if rec is None:
            return {"error": "not found", "id": item_id}
        return rec

    def stats(self) -> dict[str, Any]:
        return md.get_stats(self._conn)

    def feed(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        return md.list_messages(self._conn, dict(params))

    def healthz(self) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            ms = (time.perf_counter() - t0) * 1000
            return healthz_response(
                namespace=NAMESPACE,
                database=str(self._db_path),
                elapsed_ms=ms,
                ok=True,
            )
        except Exception as exc:  # noqa: BLE001
            ms = (time.perf_counter() - t0) * 1000
            return healthz_response(
                namespace=NAMESPACE,
                database=str(self._db_path),
                elapsed_ms=ms,
                ok=False,
                error=str(exc),
            )

    # ── Substrate-specific (routed by MessagesHandler) ───────────────
    def search(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        return md.search_messages(self._conn, dict(params))

    def facets(self) -> dict[str, Any]:
        return md.get_facets(self._conn)

    def signature(self) -> str:
        return md.signature(self._db_path)
