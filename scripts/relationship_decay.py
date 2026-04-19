# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Relationship Decay Engine — Sprint 2

Computes per-dyad temporal decay scores from the messages DB.
Cross-platform unified: resets on ANY platform contact.
Dunbar tier classification by historical interaction frequency.
8× rule alarm: flags when gap > 8× historical average inter-contact interval.

Sources:
  - Miritello et al. 2013 (9 billion phone calls) — 8× rule
  - Dunbar 2021 (Royal Society) — tier structure
  - temporal-communication-dynamics study — decay model

Usage:
    uv run relationship_decay.py [--output PATH] [--top N] [--json] [--csv]
"""

import argparse
import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------

MESSAGES_DB = Path.home() / ".claude/local/messages/messages.db"
OUTPUT_DIR = Path.home() / ".claude/local/messages/decay"

# Dunbar tier thresholds (by total message count rank)
DUNBAR_TIERS = [
    (5,   "support_clique",  14),   # top 5: half-life 14 days
    (15,  "sympathy_group",  21),   # next 10: half-life 21 days
    (50,  "band",            30),   # next 35: half-life 30 days
    (150, "tribe",           60),   # next 100: half-life 60 days
]
DEFAULT_TIER = ("acquaintance", 90)  # 500+: half-life 90 days

# 8× rule: gap > 8× average inter-contact interval = alarm
ALARM_MULTIPLIER = 8.0

# Shawn's known sender IDs (exclude from "contacts")
SHAWN_PLATFORM_IDS = {
    "telegram:user:1441369482",
    "signal:user:1e140684-393f-4124-9b22-6bf4c080e082",
    "signal:user:d98a3b57-046d-416f-9679-778c31f3d4b0",
    "signal:user:b70d5672-a484-43a9-b1b0-f238d306f2a6",
    "email:user:shawn@longtailfinancial.com",
    "email:user:swanders@ualberta.ca",
    "email:user:shawn@cadlabs.org",
    "email:user:shawn_anderson@sfu.ca",
    "email:user:notifications@github.com",
    "email:user:neo@unify.org",
    "email:user:shawn@centree.org",
    "email:user:shawn@block.science",
    "email:user:shawn@regen.network",
    "email:user:shawn@vsuccessai.com",
    "email:user:shawnltf@pm.me",
    "email:user:shawnltf@protonmail.com",
    "email:user:shawn@kwaxala.com",
    "slack:T0985PKDPU7:user:U0985PKDXSB",
}


def parse_ts(ts_str: str) -> float:
    """Parse ISO timestamp to epoch seconds. Handles Z and +00:00."""
    if not ts_str:
        return 0.0
    ts_str = ts_str.replace("Z", "+00:00")
    # Handle milliseconds in .000Z format
    try:
        dt = datetime.fromisoformat(ts_str)
    except ValueError:
        # Try stripping subseconds
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(ts_str, fmt)
                break
            except ValueError:
                continue
        else:
            return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def get_unified_contacts(db: sqlite3.Connection) -> dict:
    """
    Build unified contact profiles using identity resolution.
    Returns dict: identity_id -> {display_name, platforms, messages: [(ts, platform, direction)]}
    """
    contacts = {}

    # Phase 1: Contacts WITH identity resolution (cross-platform)
    rows = db.execute("""
        SELECT
            i.id AS identity_id,
            i.display_name,
            il.platform,
            il.platform || ':' || il.platform_id AS sender_key,
            m.platform_ts,
            m.direction
        FROM identities i
        JOIN identity_links il ON il.identity_id = i.id
        JOIN messages m ON m.sender_id = il.platform || ':' || il.platform_id
        WHERE il.platform || ':' || il.platform_id NOT IN ({})
        ORDER BY i.id, m.platform_ts
    """.format(",".join(f"'{s}'" for s in SHAWN_PLATFORM_IDS))).fetchall()

    for identity_id, display_name, platform, sender_key, ts, direction in rows:
        if identity_id not in contacts:
            contacts[identity_id] = {
                "display_name": display_name,
                "platforms": set(),
                "messages": [],
            }
        contacts[identity_id]["platforms"].add(platform)
        contacts[identity_id]["messages"].append((parse_ts(ts), platform, direction or "unknown"))

    # Phase 2: Contacts WITHOUT identity resolution (fallback to contacts table)
    resolved_sender_keys = set()
    for row in db.execute("""
        SELECT il.platform || ':' || il.platform_id FROM identity_links il
    """).fetchall():
        resolved_sender_keys.add(row[0])

    unresolved = db.execute("""
        SELECT
            c.id AS contact_id,
            c.display_name,
            c.platform,
            m.platform_ts,
            m.direction
        FROM contacts c
        JOIN messages m ON m.sender_id = c.id
        WHERE c.id NOT IN ({})
          AND c.display_name NOT LIKE '%Shawn%'
        ORDER BY c.id, m.platform_ts
    """.format(",".join(f"'{s}'" for s in SHAWN_PLATFORM_IDS))).fetchall()

    for contact_id, display_name, platform, ts, direction in unresolved:
        if contact_id in resolved_sender_keys:
            continue  # Already resolved via identity
        key = f"contact:{contact_id}"
        if key not in contacts:
            contacts[key] = {
                "display_name": display_name or contact_id,
                "platforms": set(),
                "messages": [],
            }
        contacts[key]["platforms"].add(platform)
        contacts[key]["messages"].append((parse_ts(ts), platform, direction or "unknown"))

    return contacts


def classify_dunbar_tier(rank: int) -> tuple[str, int]:
    """Return (tier_name, halflife_days) based on rank."""
    for threshold, tier_name, halflife in DUNBAR_TIERS:
        if rank <= threshold:
            return tier_name, halflife
    return DEFAULT_TIER


def compute_decay(
    messages: list[tuple[float, str, str]],
    halflife_days: float,
    now: float,
) -> float:
    """
    Compute decay signal strength: S(t) = Σ_k exp(-λ · (t - t_k))
    where λ = ln(2) / halflife_days (in seconds).
    """
    lambda_decay = math.log(2) / (halflife_days * 86400)
    signal = 0.0
    for ts, _platform, _direction in messages:
        if ts <= 0:
            continue
        dt = now - ts
        if dt < 0:
            dt = 0
        signal += math.exp(-lambda_decay * dt)
    return signal


def compute_8x_alarm(
    messages: list[tuple[float, str, str]],
    now: float,
) -> dict:
    """
    8× rule: if current gap > 8× historical average inter-contact interval → alarm.
    Returns {average_interval_days, current_gap_days, ratio, alarm}.
    """
    timestamps = sorted(set(ts for ts, _, _ in messages if ts > 0))
    if len(timestamps) < 3:
        # Not enough data for meaningful interval
        last = timestamps[-1] if timestamps else 0
        gap = (now - last) / 86400 if last > 0 else float("inf")
        return {
            "average_interval_days": None,
            "current_gap_days": round(gap, 1),
            "ratio": None,
            "alarm": False,
            "reason": "insufficient_data",
        }

    # Compute intervals between consecutive contacts
    intervals = []
    for i in range(1, len(timestamps)):
        intervals.append(timestamps[i] - timestamps[i - 1])

    avg_interval = sum(intervals) / len(intervals)
    avg_interval_days = avg_interval / 86400

    current_gap = now - timestamps[-1]
    current_gap_days = current_gap / 86400

    ratio = current_gap / avg_interval if avg_interval > 0 else float("inf")

    return {
        "average_interval_days": round(avg_interval_days, 1),
        "current_gap_days": round(current_gap_days, 1),
        "ratio": round(ratio, 1),
        "alarm": ratio >= ALARM_MULTIPLIER,
        "reason": "8x_exceeded" if ratio >= ALARM_MULTIPLIER else "normal",
    }


def compute_outbound_ratio(messages: list[tuple[float, str, str]]) -> dict:
    """Compute sent vs received breakdown."""
    sent = sum(1 for _, _, d in messages if d == "sent")
    received = sum(1 for _, _, d in messages if d == "received")
    total = len(messages)
    return {
        "sent": sent,
        "received": received,
        "total": total,
        "outbound_ratio": round(sent / total, 3) if total > 0 else 0,
    }


def compute_recent_activity(
    messages: list[tuple[float, str, str]],
    now: float,
    window_days: int = 30,
) -> dict:
    """Messages in last N days."""
    cutoff = now - window_days * 86400
    recent = [m for m in messages if m[0] >= cutoff]
    sent = sum(1 for _, _, d in recent if d == "sent")
    received = sum(1 for _, _, d in recent if d == "received")
    return {"last_30d_total": len(recent), "last_30d_sent": sent, "last_30d_received": received}


def main():
    parser = argparse.ArgumentParser(description="Relationship Decay Engine")
    parser.add_argument("--output", type=str, help="Output file path")
    parser.add_argument("--top", type=int, default=50, help="Number of contacts to include")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--csv", action="store_true", help="Output CSV")
    parser.add_argument("--alarms-only", action="store_true", help="Only show contacts in alarm state")
    args = parser.parse_args()

    if not MESSAGES_DB.exists():
        print(f"Error: messages DB not found at {MESSAGES_DB}", file=sys.stderr)
        sys.exit(1)

    db = sqlite3.connect(str(MESSAGES_DB))
    now = datetime.now(timezone.utc).timestamp()

    print("Building unified contact profiles...")
    contacts = get_unified_contacts(db)
    print(f"  {len(contacts)} unique contacts found")

    # Sort by total message count for Dunbar tier assignment
    ranked = sorted(contacts.items(), key=lambda x: len(x[1]["messages"]), reverse=True)

    results = []
    for rank, (cid, contact) in enumerate(ranked, 1):
        msgs = contact["messages"]
        if not msgs:
            continue

        tier_name, halflife = classify_dunbar_tier(rank)
        decay_score = compute_decay(msgs, halflife, now)
        alarm = compute_8x_alarm(msgs, now)
        volume = compute_outbound_ratio(msgs)
        recent = compute_recent_activity(msgs, now)

        timestamps = sorted(ts for ts, _, _ in msgs if ts > 0)
        last_ts = timestamps[-1] if timestamps else 0
        first_ts = timestamps[0] if timestamps else 0

        entry = {
            "rank": rank,
            "identity_id": cid,
            "display_name": contact["display_name"],
            "platforms": sorted(contact["platforms"]),
            "dunbar_tier": tier_name,
            "halflife_days": halflife,
            "decay_score": round(decay_score, 3),
            "last_contact": datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat() if last_ts else None,
            "first_contact": datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat() if first_ts else None,
            "days_since_contact": round((now - last_ts) / 86400, 1) if last_ts else None,
            "alarm": alarm,
            "volume": volume,
            "recent": recent,
        }
        results.append(entry)

    # Filter alarms if requested
    if args.alarms_only:
        results = [r for r in results if r["alarm"]["alarm"]]

    # Trim to top N
    results = results[: args.top]

    # Compute summary stats
    total_alarms = sum(1 for r in ranked if compute_8x_alarm(r[1]["messages"], now)["alarm"])
    tier_counts = {}
    for rank, (_, contact) in enumerate(ranked, 1):
        tier, _ = classify_dunbar_tier(rank)
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_contacts": len(contacts),
        "total_alarms": total_alarms,
        "tier_distribution": tier_counts,
        "contacts": results,
    }

    # Output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.output or str(OUTPUT_DIR / "decay-report.json")

    if args.json or out_path.endswith(".json"):
        with open(out_path, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\nReport written to {out_path}")
    elif args.csv:
        import csv
        csv_path = out_path.replace(".json", ".csv") if out_path.endswith(".json") else out_path
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "rank", "display_name", "platforms", "tier", "halflife",
                "decay_score", "days_since", "alarm", "gap_ratio",
                "total_msgs", "sent", "received", "last_30d",
            ])
            for r in results:
                writer.writerow([
                    r["rank"], r["display_name"], "|".join(r["platforms"]),
                    r["dunbar_tier"], r["halflife_days"], r["decay_score"],
                    r["days_since_contact"], r["alarm"]["alarm"],
                    r["alarm"].get("ratio", ""),
                    r["volume"]["total"], r["volume"]["sent"], r["volume"]["received"],
                    r["recent"]["last_30d_total"],
                ])
        print(f"\nCSV written to {csv_path}")
    else:
        # Human-readable table
        print(f"\n{'='*100}")
        print(f"RELATIONSHIP DECAY REPORT — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
        print(f"{'='*100}")
        print(f"Total contacts: {len(contacts)} | Alarms: {total_alarms}")
        print(f"Tiers: {tier_counts}")
        print(f"{'='*100}\n")

        print(f"{'Rk':>3} {'Name':<30} {'Tier':<16} {'Decay':>8} {'Days':>6} {'Alarm':>6} {'Ratio':>6} {'30d':>5} {'Platforms'}")
        print("-" * 100)
        for r in results:
            alarm_flag = "⚠️" if r["alarm"]["alarm"] else ""
            ratio = r["alarm"].get("ratio", "")
            ratio_str = f"{ratio}" if ratio else ""
            print(
                f"{r['rank']:>3} {r['display_name'][:29]:<30} {r['dunbar_tier']:<16} "
                f"{r['decay_score']:>8.1f} {r['days_since_contact']:>6.1f} {alarm_flag:>6} "
                f"{ratio_str:>6} {r['recent']['last_30d_total']:>5} {','.join(r['platforms'])}"
            )

    # Always write JSON for morning brief consumption
    json_path = str(OUTPUT_DIR / "decay-report.json")
    if out_path != json_path:
        with open(json_path, "w") as f:
            json.dump(output, f, indent=2, default=str)

    db.close()

    # Print top 5 alarms for quick visibility
    alarms = [r for r in results if r["alarm"]["alarm"]]
    if alarms:
        print(f"\n⚠️  TOP ALARMS ({len(alarms)} contacts):")
        for r in alarms[:5]:
            ratio = r["alarm"].get("ratio", "?")
            print(
                f"  {r['display_name']}: {r['days_since_contact']:.0f}d since contact, "
                f"{ratio}× avg interval, tier={r['dunbar_tier']}"
            )

    print(f"\nDecay engine complete. {len(contacts)} contacts analyzed, {total_alarms} alarms.")


if __name__ == "__main__":
    main()
