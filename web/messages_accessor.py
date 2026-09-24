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
import salience as sal
from claude_webui.healthz import healthz_response

__version__ = "0.1.0"

NAMESPACE = "legion.claude-message"


class MessagesAccessor:
    """Substrate accessor for the claude-messages webui (Phase 1: read-only)."""

    NOISE_THRESHOLD = 0.35
    SIGNAL_POOL_FACTOR = 6
    SIGNAL_POOL_MAX = 600

    def __init__(self, db_path: Path | str | None = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else Path(md.DEFAULT_DB_PATH)
        self._conn = md.connect_ro(self._db_path)
        self._engaged: set[str] | None = None
        self._engaged_sig: str | None = None
        # signature-keyed memo for cheap-but-not-free aggregates (facets/stats).
        # They only change when the DB changes, so a stable DB → instant repeats.
        self._cache: dict[str, tuple[str, Any]] = {}

    def _cached(self, key: str, fn: Any) -> Any:
        sig = self.signature()
        hit = self._cache.get(key)
        if hit is not None and hit[0] == sig:
            return hit[1]
        val = fn()
        self._cache[key] = (sig, val)
        return val

    # ── Salience helpers ─────────────────────────────────────────────
    def _ctx(self) -> dict:
        # Recompute the engaged-thread set only when the DB actually changes.
        sig = self.signature()
        if self._engaged is None or self._engaged_sig != sig:
            self._engaged = md.engaged_thread_ids(self._conn)
            self._engaged_sig = sig
        return {"engaged_threads": self._engaged}

    def _annotate(self, cards: list[dict]) -> list[dict]:
        ctx = self._ctx()
        for c in cards:
            s = sal.salience(c, ctx)
            c["salience"] = s["salience"]
            c["salience_reasons"] = s["reasons"]
        return cards

    # ── Accessor Protocol ────────────────────────────────────────────
    def list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        p = dict(params)
        sort = str(p.pop("sort", "recent")).lower()
        hide = str(p.pop("hide_noise", "")).lower() in ("1", "true", "yes")
        try:
            limit = int(p.get("limit", md.DEFAULT_LIMIT))
        except (TypeError, ValueError):
            limit = md.DEFAULT_LIMIT
        try:
            offset = int(p.get("offset", 0))
        except (TypeError, ValueError):
            offset = 0
        if sort == "signal":
            pool = dict(p)
            pool["limit"] = min(max(limit * self.SIGNAL_POOL_FACTOR, 120), self.SIGNAL_POOL_MAX)
            pool["offset"] = 0
            cards = self._annotate(md.list_messages(self._conn, pool))
            cards.sort(key=lambda c: c["salience"], reverse=True)
            if hide:
                cards = [c for c in cards if c["salience"] >= self.NOISE_THRESHOLD]
            return cards[offset:offset + limit]
        cards = self._annotate(md.list_messages(self._conn, p))
        if hide:
            cards = [c for c in cards if c["salience"] >= self.NOISE_THRESHOLD]
        return cards

    def detail(self, item_id: str) -> dict[str, Any]:
        rec = md.get_message_detail(self._conn, item_id)
        if rec is None:
            return {"error": "not found", "id": item_id}
        s = sal.salience(rec, self._ctx())
        rec["salience"] = s["salience"]
        rec["salience_reasons"] = s["reasons"]
        return rec

    def stats(self) -> dict[str, Any]:
        return self._cached("stats", lambda: md.get_stats(self._conn))

    def feed(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        return self.list(dict(params))

    def healthz(self) -> dict[str, Any]:
        t0 = time.perf_counter()
        try:
            n = self._conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            ms = (time.perf_counter() - t0) * 1000
            resp = healthz_response(
                namespace=NAMESPACE,
                database=str(self._db_path),
                elapsed_ms=ms,
                ok=True,
            )
            # Surface a key_metric so the claude-home aggregator tile shows a count.
            resp["stats"] = {"key_metric": n, "key_metric_label": "messages"}
            return resp
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
        p = dict(params)
        sort = str(p.pop("sort", "recent")).lower()
        hide = str(p.pop("hide_noise", "")).lower() in ("1", "true", "yes")
        cards = self._annotate(md.search_messages(self._conn, p))
        if sort == "signal":
            cards.sort(key=lambda c: c["salience"], reverse=True)
        if hide:
            cards = [c for c in cards if c["salience"] >= self.NOISE_THRESHOLD]
        return cards

    def facets(self) -> dict[str, Any]:
        return self._cached("facets", lambda: md.get_facets(self._conn))

    def threads(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        cards = md.list_threads(self._conn, dict(params))
        ctx = self._ctx()
        for c in cards:
            shaped = {
                "thread_id": c["id"], "thread_type": c["thread_type"],
                "sender_id": c.get("last_sender"), "sender_name": c.get("title"),
                "content": c.get("last_content"), "connectedness": None,
            }
            s = sal.salience(shaped, ctx)
            c["salience"] = s["salience"]
            c["salience_reasons"] = s["reasons"]
        sort = str(params.get("sort") or "last_activity").lower()
        if sort == "salience":
            cards.sort(key=lambda c: c.get("salience", 0), reverse=True)
        elif sort == "activity":
            cards.sort(key=lambda c: c.get("message_count", 0), reverse=True)
        elif sort == "people":
            tier_rank = {"support_clique": 5, "sympathy_group": 4, "affinity_group": 3,
                         "active_network": 2, "acquaintance": 1}
            cards.sort(key=lambda c: max([0] + [tier_rank.get(p.get("dunbar_layer"), 0)
                                                for p in c.get("participants", [])]), reverse=True)
        elif sort == "unread":
            cards.sort(key=lambda c: c.get("needs_reply", False), reverse=True)
        return cards

    def thread(self, thread_id: str) -> dict[str, Any]:
        t = md.get_thread(self._conn, thread_id)
        if t is None:
            return {"error": "not found", "id": thread_id}
        t["messages"] = self._annotate(t["messages"])
        return t

    def signature(self) -> str:
        return md.signature(self._db_path)
