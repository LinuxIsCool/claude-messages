"""claude-messages webui — thin satellite of claude_webui.WebuiKernel.

Mode A standalone:   python web/server.py --port 8895
Mode B mount:        build_kernel(port=0) for the claude-webui Platform.

Phase 1: read-only Messages surface. Real-time SSE via signature poll on
messages.db (+ inotify when available). No mutations.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from claude_webui.kernel import WebuiHandler, WebuiKernel

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from messages_accessor import MessagesAccessor  # noqa: E402
from messages_handler import MessagesHandler  # noqa: E402

__version__ = "0.1.0"
DEFAULT_PORT = 8895


class MessagesKernel(WebuiKernel):
    """WebuiKernel that wires MessagesHandler. Per-instance subclass keeps the
    multi-tenant invariant (no shared class state between kernels)."""

    def _make_handler_class(self) -> type[WebuiHandler]:
        accessor = self.accessor
        static_dir = self.static_dir
        event_bus = self._event_bus

        class _Handler(MessagesHandler):
            pass

        _Handler.accessor = accessor  # type: ignore[attr-defined]
        _Handler.static_dir = static_dir  # type: ignore[attr-defined]
        _Handler.event_bus = event_bus  # type: ignore[attr-defined]
        return _Handler


def build_kernel(port: int = DEFAULT_PORT, bind: str = "127.0.0.1") -> MessagesKernel:
    accessor = MessagesAccessor()
    db_path = accessor._db_path  # noqa: SLF001 — watch the real DB file
    return MessagesKernel(
        accessor=accessor,
        port=port,
        bind=bind,
        static_dir=HERE,
        signature_fn=accessor.signature,
        watch_paths=[db_path.parent] if db_path.parent.exists() else None,
        poll_interval_s=2.0,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="claude-messages webui")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", str(DEFAULT_PORT))))
    parser.add_argument("--bind", default="127.0.0.1")
    args = parser.parse_args()
    return build_kernel(port=args.port, bind=args.bind).serve()


if __name__ == "__main__":
    raise SystemExit(main())
