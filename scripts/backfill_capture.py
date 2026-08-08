#!/usr/bin/env python3
"""Backfill capture provenance onto messages.

    python3 scripts/backfill_capture.py            # dry run
    python3 scripts/backfill_capture.py --apply

Gives 47k messages a derived `uuid7` so they sort chronologically across
platforms and share one address space with prompts, subagents, artifacts and
meetings, and stamps `kind=received`.

## Why this is backfill-only, and what that costs

The messages write path is TypeScript (`server/src/`). Stamping at capture
would need a second implementation of the `legion_capture` contract in TS —
which is precisely the divergence that "one shared library" was chosen to
avoid. Two implementations of one rule drift, and the drift is silent.

So every row here is `capture_source='backfill'`, and messages captured after
this run will have **no** stamp until either the TS port lands or a scheduled
pass re-runs this script. That is a real gap, deliberately visible in the data
rather than hidden by a plausible-looking value:

    SELECT capture_source, count(*) FROM messages GROUP BY 1;

`kind=received` says the content was authored outside Legion and ingested by a
sync adapter. It is **not** a submit kind: nobody submitted a Signal message
*here*. Direction (who authored it) stays in `messages.direction`, which
already carries `sent | received | unknown` honestly — folding it into `kind`
would give one column two meanings.

Ids derive from `(platform_ts, platform, id)`, so re-running is free and
produces byte-identical values.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS))

# Reuse the meetings bootstrap rather than adding a fourth copy.
_SUPER = _SCRIPTS.parents[2]
sys.path.insert(0, str(_SUPER / "packages/legion-capture/src"))

from legion_capture import (  # noqa: E402
    Discriminator, IdentityError, Kind, guard, is_valid, uuid7,
)

MESSAGES_DB = Path.home() / ".claude/local/messages/messages.db"

CAPTURE_COLUMNS = (
    ("uuid7", "TEXT"),
    ("kind", "TEXT"),
    ("discriminator", "TEXT"),
    ("capture_source", "TEXT"),
    ("captured_at", "TIMESTAMP"),
)


def migrate(conn: sqlite3.Connection) -> None:
    for col, decl in CAPTURE_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {decl}")
        except sqlite3.OperationalError:
            pass
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_uuid7 "
                     "ON messages(uuid7) WHERE uuid7 IS NOT NULL")
    except sqlite3.OperationalError:
        pass
    conn.commit()


def backfill(db_path: Path, apply: bool, batch: int = 5000) -> dict:
    conn = sqlite3.connect(str(db_path))
    stats = {"rows": 0, "stamped": 0, "already": 0, "bad_ts": 0}
    try:
        migrate(conn)
        rows = conn.execute(
            "SELECT id, platform, platform_ts, uuid7 FROM messages").fetchall()
        pending = []
        for mid, platform, ts, existing in rows:
            stats["rows"] += 1
            if existing:
                stats["already"] += 1
                continue
            try:
                uid = uuid7(ts, platform or "", mid or "")
            except IdentityError:
                # An unparseable platform timestamp is a defect in the row.
                # Skipping beats stamping an id that claims 1970 and then
                # sorts to the top of every chronological view.
                stats["bad_ts"] += 1
                continue
            pending.append((uid, str(Kind.RECEIVED),
                            str(Discriminator.CHANNEL), ts, mid))
            stats["stamped"] += 1
            if apply and len(pending) >= batch:
                _flush(conn, pending)
                pending = []
        if apply and pending:
            _flush(conn, pending)
        if apply:
            conn.commit()
    finally:
        conn.close()
    return stats


def _flush(conn, pending):
    conn.executemany(
        "UPDATE messages SET uuid7=?, kind=?, discriminator=?, "
        "capture_source='backfill', captured_at=? WHERE id=?", pending)
    conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--db", type=Path, default=MESSAGES_DB)
    a = ap.parse_args()

    if not a.db.exists():
        print(f"no messages database at {a.db}")
        return 1

    s = backfill(a.db, a.apply)
    verb = "stamped" if a.apply else "would stamp"
    print(f"messages        : {s['rows']:,}")
    print(f"{verb:16}: {s['stamped']:,}")
    print(f"already stamped : {s['already']:,}")
    if s["bad_ts"]:
        print(f"unusable ts     : {s['bad_ts']:,}  (skipped, not stamped 1970)")
    if not a.apply:
        print("\ndry run. re-run with --apply to write.")
    else:
        print("\nNOTE: capture-time stamping is NOT active — the write path is "
              "TypeScript.\nNew messages will have capture_source NULL until a "
              "TS port lands or this\nscript runs again. Check with:\n"
              "  SELECT capture_source, count(*) FROM messages GROUP BY 1;")
    return 0


if __name__ == "__main__":
    sys.exit(main())
