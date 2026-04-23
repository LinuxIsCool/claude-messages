#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "claude-llms @ file:///home/shawn/.claude/plugins/local/legion-plugins/plugins/claude-llms",
# ]
# ///
"""
Thread summarization via claude-llms.

Input (stdin JSON):
  { "messages": [{ "sender_name": "...", "content": "...", "platform_ts": "..." }] }

Output (stdout JSON):
  { "summary": "...", "model": "telus:gemma-4-31b-it" }

No direct TELUS env-var reads. Routing + fallback + spend tracking are
handled by `claude_llms.api.complete()`. Backlog 247 migration, 2026-04-22.
"""

import asyncio
import json
import sys

from claude_llms.api import complete

SYSTEM_PROMPT = (
    "Summarize this conversation in 2-3 sentences. "
    "Include key topics, decisions, and any open questions. "
    "Be specific — use names and technical terms. "
    "Do not start with 'This conversation' or 'The conversation'."
)


async def summarize_async(messages: list[dict]) -> tuple[str, str]:
    """Call claude_llms to summarize messages. Returns (summary, model_used)."""
    # Build conversation text from last 100 messages
    lines = []
    for m in messages[-100:]:
        name = m.get("sender_name") or m.get("sender_id") or "Unknown"
        ts = (m.get("platform_ts") or "")[:16]
        content = m.get("content") or ""
        if content:
            lines.append(f"[{ts}] {name}: {content}")

    if not lines:
        return "(empty thread)", "none"

    conversation = "\n".join(lines)

    # Truncate to ~8K chars to stay within context limits
    if len(conversation) > 8000:
        conversation = conversation[-8000:]

    try:
        result = await complete(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": conversation},
            ],
            task_type="summarization",
            sovereignty=True,
            max_tokens=200,
            temperature=0.3,
            caller="claude-messages:summarize",
        )
        summary = (result.content or "").strip()
        if not summary:
            return "(summary unavailable — empty response)", result.model or "unknown"
        return summary, result.model or "unknown"
    except Exception as e:  # noqa: BLE001
        return f"(summary unavailable — {type(e).__name__})", "none"


def summarize(messages: list[dict]) -> tuple[str, str]:
    return asyncio.run(summarize_async(messages))


def main():
    data = json.load(sys.stdin)
    summary, model_used = summarize(data.get("messages", []))
    json.dump({"summary": summary, "model": model_used}, sys.stdout)


if __name__ == "__main__":
    main()
