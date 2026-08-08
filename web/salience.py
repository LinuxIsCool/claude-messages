"""Salience scorer framework for claude-messages (task-4071 Layer 0).

Evergreen design: a registry of independent SIGNAL functions, each mapping a
message + context to a (value in [0,1], human reason). salience() blends them
by a weight vector and returns the score PLUS a transparent breakdown. New
signals register via @signal without touching the blender — that is the
extension point for Layers 1-3 (behavioral, learned, semantic).
"""
from __future__ import annotations

from typing import Any, Callable

# A signal: (message: dict, ctx: dict) -> (value in [0,1], reason: str)
SignalFn = Callable[[dict[str, Any], dict[str, Any]], "tuple[float, str]"]

SIGNALS: dict[str, SignalFn] = {}

# Default blend weights. One dict, the single tuning surface. Later: learned.
DEFAULT_WEIGHTS: dict[str, float] = {
    "reciprocity": 0.34,
    "channel_type": 0.22,
    "relationship": 0.22,
    "identity": 0.12,
    "content_quality": 0.10,
}


def signal(name: str) -> Callable[[SignalFn], SignalFn]:
    """Decorator: register a signal under `name`."""
    def deco(fn: SignalFn) -> SignalFn:
        SIGNALS[name] = fn
        return fn
    return deco


def _clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


# ── Layer-0 signals ─────────────────────────────────────────────────

@signal("reciprocity")
def _reciprocity(msg: dict[str, Any], ctx: dict[str, Any]) -> tuple[float, str]:
    """Has Shawn ever sent into this thread? Engaged threads are signal;
    one-way firehoses are noise. The single strongest cheap signal."""
    engaged = ctx.get("engaged_threads") or set()
    tid = msg.get("thread_id")
    if tid and tid in engaged:
        return 1.0, "you have replied in this thread"
    return 0.12, "you have never replied in this thread"


@signal("channel_type")
def _channel_type(msg: dict[str, Any], ctx: dict[str, Any]) -> tuple[float, str]:
    """DM > small group > large group > broadcast/channel."""
    tt = (msg.get("thread_type") or "").lower()
    table = {
        "dm": (1.0, "direct message"),
        "direct": (1.0, "direct message"),
        "private": (0.85, "private chat"),
        "group": (0.5, "group chat"),
        "supergroup": (0.3, "large group"),
        "channel": (0.1, "broadcast channel"),
        "broadcast": (0.1, "broadcast channel"),
    }
    if tt in table:
        return table[tt]
    return 0.4, "unknown channel type"


@signal("identity")
def _identity(msg: dict[str, Any], ctx: dict[str, Any]) -> tuple[float, str]:
    """A resolved human name beats a raw platform handle."""
    name = msg.get("sender_name")
    sid = msg.get("sender_id")
    if name and name != sid:
        return 1.0, f"known sender: {name}"
    return 0.25, "unresolved sender handle"


@signal("relationship")
def _relationship(msg: dict[str, Any], ctx: dict[str, Any]) -> tuple[float, str]:
    """ContactRank composite (connectedness) is the relationship prior."""
    c = msg.get("connectedness")
    if c is None:
        return 0.3, "no relationship score"
    tier = msg.get("priority_tier")
    label = f"relationship {round(float(c) * 100)}%" + (f" · {tier}" if tier else "")
    return _clamp01(float(c)), label


@signal("content_quality")
def _content_quality(msg: dict[str, Any], ctx: dict[str, Any]) -> tuple[float, str]:
    """Cheap noise penalty: link/emoji density, all-caps, length blow-out.
    Returns CLEANLINESS (1 - noise)."""
    text = (msg.get("content") or "").strip()
    if not text:
        return 0.5, "empty / non-text"
    import re
    penalty = 0.0
    notes = []
    links = len(re.findall(r"https?://", text))
    if links >= 1:
        penalty += min(0.4, 0.2 * links)
        notes.append(f"{links} link(s)")
    emoji = len(re.findall(r"[\U0001F000-\U0001FAFF☀-➿]", text))
    if emoji >= 4:
        penalty += min(0.35, 0.05 * emoji)
        notes.append(f"{emoji} emoji")
    letters = [c for c in text if c.isalpha() and c.isascii()]
    if len(letters) >= 12 and sum(c.isupper() for c in letters) / len(letters) > 0.7:
        penalty += 0.25
        notes.append("mostly caps")
    value = _clamp01(1.0 - penalty)
    reason = "clean content" if not notes else "noisy: " + ", ".join(notes)
    return value, reason


# ── Blender ─────────────────────────────────────────────────────────

def salience(
    msg: dict[str, Any],
    ctx: dict[str, Any] | None = None,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Blend all registered signals into a [0,1] salience + breakdown.

    Returns {"salience": float, "reasons": [{signal, value, weight,
    contribution, reason}, ...]} sorted by contribution desc. Unknown signal
    names in `weights` are ignored; signals with no weight default to 0.
    """
    ctx = ctx or {}
    weights = weights or DEFAULT_WEIGHTS
    total_w = sum(weights.get(name, 0.0) for name in SIGNALS) or 1.0
    reasons: list[dict[str, Any]] = []
    acc = 0.0
    for name, fn in SIGNALS.items():
        w = weights.get(name, 0.0)
        if w == 0.0:
            continue
        value, reason = fn(msg, ctx)
        value = _clamp01(float(value))
        contribution = value * w
        acc += contribution
        reasons.append({
            "signal": name, "value": round(value, 3), "weight": w,
            "contribution": round(contribution / total_w, 3), "reason": reason,
        })
    reasons.sort(key=lambda r: r["contribution"], reverse=True)
    return {"salience": round(acc / total_w, 3), "reasons": reasons}
