# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""
Venture-Thread Mapper — Builds the missing link between ventures and communication threads.

Two strategies:
  1. Keyword matching: thread titles → venture keywords
  2. Co-venturer presence: thread participants → venture co-venturers

Produces: ~/.claude/local/messages/venture-threads.json

Usage:
    uv run venture_threads.py [--output PATH] [--json]
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

MESSAGES_DB = Path.home() / ".claude/local/messages/messages.db"
VENTURES_DIR = Path.home() / ".claude/local/ventures/active"
OUTPUT_DIR = Path.home() / ".claude/local/messages"

# Venture keyword patterns for thread title matching
# Built from venture YAML titles, co-venturer names, and known thread names
VENTURE_KEYWORDS = {
    "bcrg": [
        "bcrg", "bonding curve", "avalanche", "cadcad", "avax",
        "infrabuild", "infrabridge",
    ],
    "regen-ai": [
        "regen", "gaia", "protocol politician", "seatrees", "sea trees",
        "heartbeat", "regen network", "regen ai", "regen coin",
        "regen commons", "regenlearnings",
    ],
    "indigenomics-ai": [
        "indigenomics", "indigenous", "iai devs", "gpu - indigenomics",
        "neu/indigenomics", "neu jam", "impact jam",
    ],
    "salish-sea-dreaming": [
        "salish", "herring", "salish sea", "m37", "moonfish",
        "autolume", "touchdesigner",
    ],
    "ecoscene-oasis": [
        "ecoscene", "oasis", "agora",
    ],
    "cascadia-systems": [
        "cascadia", "cascadia systems",
    ],
    "symbiocene-labs": [
        "symbiocene",
    ],
    "kwaxala": [
        "kwaxala", "beyeing", "pete cork", "tgna",
    ],
    "longtail-financial": [
        "longtail", "ltf", "kyb", "t2 filing",
    ],
}


def load_co_venturer_ids(db: sqlite3.Connection) -> dict[str, set[str]]:
    """
    Load co-venturer platform IDs from venture YAML + identity resolution.
    Returns: venture_id → set of sender_id strings
    """
    venture_ids = {}

    for vf in sorted(VENTURES_DIR.glob("*.md")):
        text = vf.read_text()
        parts = text.split("---", 2)
        if len(parts) < 3:
            continue
        try:
            data = yaml.safe_load(parts[1])
        except Exception:
            continue
        if not data or not isinstance(data, dict):
            continue

        vid = data.get("id", vf.stem)
        sender_ids = set()

        for cv in data.get("co_venturers", []):
            contact = cv.get("contact", "")
            name = cv.get("name", "")

            # Direct contact field (telegram:user:123456)
            if contact:
                for c in contact.split(","):
                    c = c.strip()
                    if ":" in c and not c.startswith("http"):
                        sender_ids.add(c)

            # Try identity resolution by name
            if name and "shawn" not in name.lower():
                # Fuzzy match on identity display_name
                first_word = name.split("(")[0].strip().split()[0]
                rows = db.execute(
                    "SELECT il.platform || ':' || il.platform_id "
                    "FROM identity_links il "
                    "JOIN identities i ON il.identity_id = i.id "
                    "WHERE LOWER(i.display_name) LIKE ?",
                    (f"%{first_word.lower()}%",),
                ).fetchall()
                for row in rows:
                    sender_ids.add(row[0])

        venture_ids[vid] = sender_ids

    return venture_ids


def build_venture_thread_map(db: sqlite3.Connection) -> dict:
    """
    Build venture → thread mapping using keyword matching + co-venturer presence.
    """
    # Load co-venturer sender IDs
    co_venturer_ids = load_co_venturer_ids(db)

    # Get all threads with recent activity
    threads = db.execute("""
        SELECT t.id, t.platform, t.title, t.thread_type,
               MAX(m.platform_ts) as last_activity,
               COUNT(m.id) as msg_count
        FROM threads t
        JOIN messages m ON m.thread_id = t.id
        WHERE m.platform_ts > '2026-01-01'
        GROUP BY t.id
        ORDER BY last_activity DESC
    """).fetchall()

    mapping = {}  # venture_id → [thread_info]
    thread_ventures = {}  # thread_id → [venture_ids]

    for thread_id, platform, title, thread_type, last_activity, msg_count in threads:
        if not title:
            continue

        title_lower = title.lower()
        matched_ventures = set()

        # Strategy 1: Keyword matching on thread title
        for vid, keywords in VENTURE_KEYWORDS.items():
            for kw in keywords:
                if kw.lower() in title_lower:
                    matched_ventures.add(vid)
                    break

        # Strategy 2: Co-venturer presence detection
        # Check if thread has messages from venture co-venturers
        if not matched_ventures:
            for vid, sender_ids in co_venturer_ids.items():
                if not sender_ids:
                    continue
                placeholders = ",".join(f"'{s}'" for s in sender_ids)
                result = db.execute(f"""
                    SELECT COUNT(*) FROM messages
                    WHERE thread_id = ?
                    AND sender_id IN ({placeholders})
                    AND platform_ts > '2026-03-01'
                    LIMIT 1
                """, (thread_id,)).fetchone()
                if result and result[0] > 0:
                    matched_ventures.add(vid)

        if not matched_ventures:
            continue

        # Get ball-with info
        last_msg = db.execute("""
            SELECT sender_id, platform_ts, substr(content, 1, 200)
            FROM messages
            WHERE thread_id = ?
            ORDER BY platform_ts DESC
            LIMIT 1
        """, (thread_id,)).fetchone()

        # Get Shawn's last message in this thread
        shawn_ids = [
            "telegram:user:1441369482",
            "signal:user:1e140684-393f-4124-9b22-6bf4c080e082",
            "signal:user:b70d5672-a484-43a9-b1b0-f238d306f2a6",
            "email:user:shawn@longtailfinancial.com",
            "slack:T0985PKDPU7:user:U0985PKDXSB",
        ]
        shawn_placeholder = ",".join(f"'{s}'" for s in shawn_ids)
        shawn_last = db.execute(f"""
            SELECT platform_ts FROM messages
            WHERE thread_id = ? AND sender_id IN ({shawn_placeholder})
            ORDER BY platform_ts DESC LIMIT 1
        """, (thread_id,)).fetchone()

        # Determine ball-with
        last_sender = last_msg[0] if last_msg else None
        ball_with = "unknown"
        if last_sender in shawn_ids:
            ball_with = "them"
        elif last_sender:
            ball_with = "shawn"

        # Count messages since Shawn's last
        msgs_since_shawn = 0
        if shawn_last:
            result = db.execute("""
                SELECT COUNT(*) FROM messages
                WHERE thread_id = ? AND platform_ts > ? AND sender_id NOT IN ({})
            """.format(shawn_placeholder), (thread_id, shawn_last[0])).fetchone()
            msgs_since_shawn = result[0] if result else 0

        thread_info = {
            "thread_id": thread_id,
            "platform": platform,
            "title": title,
            "thread_type": thread_type,
            "last_activity": last_activity,
            "msg_count": msg_count,
            "ventures": sorted(matched_ventures),
            "ball_with": ball_with,
            "last_sender": last_sender,
            "shawn_last_msg": shawn_last[0] if shawn_last else None,
            "msgs_since_shawn": msgs_since_shawn,
        }

        thread_ventures[thread_id] = sorted(matched_ventures)
        for vid in matched_ventures:
            if vid not in mapping:
                mapping[vid] = []
            mapping[vid].append(thread_info)

    return mapping, thread_ventures


def main():
    if not MESSAGES_DB.exists():
        print(f"Error: messages DB not found at {MESSAGES_DB}", file=sys.stderr)
        sys.exit(1)

    db = sqlite3.connect(str(MESSAGES_DB))
    now = datetime.now(timezone.utc)

    print("Building venture-thread mapping...")
    mapping, thread_ventures = build_venture_thread_map(db)

    # Summary
    total_threads = sum(len(threads) for threads in mapping.values())
    ball_with_shawn = sum(
        1 for threads in mapping.values()
        for t in threads if t["ball_with"] == "shawn"
    )

    print(f"\nVenture-Thread Map:")
    for vid, threads in sorted(mapping.items()):
        bws = sum(1 for t in threads if t["ball_with"] == "shawn")
        print(f"  {vid}: {len(threads)} threads, {bws} awaiting Shawn")

    output = {
        "generated_at": now.isoformat(),
        "total_threads_mapped": total_threads,
        "ball_with_shawn": ball_with_shawn,
        "by_venture": {
            vid: {
                "thread_count": len(threads),
                "ball_with_shawn": sum(1 for t in threads if t["ball_with"] == "shawn"),
                "threads": sorted(threads, key=lambda t: t["last_activity"], reverse=True),
            }
            for vid, threads in sorted(mapping.items())
        },
    }

    out_path = OUTPUT_DIR / "venture-threads.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nTotal: {total_threads} threads mapped, {ball_with_shawn} awaiting Shawn")
    print(f"Written to {out_path}")

    db.close()


if __name__ == "__main__":
    main()
