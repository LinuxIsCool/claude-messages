"""MessagesHandler — WebuiHandler + 2 extra GET routes (/api/search, /api/facets).

Override _dispatch_get so kernel standard routes (/, /api/list, /api/detail,
/api/stats, /api/feed, /healthz, /static/*) still work AND the extras resolve
before the kernel's not-found fallback. Same shape as RhythmsHandler.
"""
from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse

from claude_webui.kernel import WebuiHandler


class MessagesHandler(WebuiHandler):
    def _dispatch_get(self) -> None:  # noqa: D401
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        accessor = self.accessor  # type: ignore[attr-defined]

        if path == "/api/search":
            raw = parse_qs(parsed.query, keep_blank_values=True)
            params = {k: v[0] if v else "" for k, v in raw.items()}
            try:
                self._send_json(accessor.search(params))
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
            return

        if path == "/api/facets":
            try:
                self._send_json(accessor.facets())
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, status=500)
            return

        super()._dispatch_get()
