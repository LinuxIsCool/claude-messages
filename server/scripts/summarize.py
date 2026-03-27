#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests>=2.31"]
# ///
"""
Thread summarization via claude-llms TELUS GPT OSS endpoint.

Input (stdin JSON):
  { "messages": [{ "sender_name": "...", "content": "...", "platform_ts": "..." }] }

Output (stdout JSON):
  { "summary": "...", "model": "telus-gpt-oss" }

Loads TELUS credentials from ~/.claude/local/secrets/telus-api.env
(same file as claude-llms plugin).
"""

import json
import os
import sys
from pathlib import Path

import requests

SECRETS_FILE = Path.home() / ".claude" / "local" / "secrets" / "telus-api.env"

SYSTEM_PROMPT = (
    "Summarize this conversation in 2-3 sentences. "
    "Include key topics, decisions, and any open questions. "
    "Be specific — use names and technical terms. "
    "Do not start with 'This conversation' or 'The conversation'."
)


def load_secrets() -> dict[str, str]:
    """Load KEY=VALUE pairs from the secrets env file."""
    secrets: dict[str, str] = {}
    if SECRETS_FILE.exists():
        for line in SECRETS_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                secrets[key.strip()] = val.strip()
    return secrets


def get_config() -> tuple[str, str, str]:
    """Resolve URL, API key, and model from env or secrets file."""
    secrets = load_secrets()

    url = os.environ.get("TELUS_GPT_OSS_URL") or secrets.get("TELUS_GPT_OSS_URL", "")
    key = os.environ.get("TELUS_GPT_OSS_KEY") or secrets.get("TELUS_GPT_OSS_KEY", "")
    model = (
        os.environ.get("TELUS_LLM_MODEL")
        or secrets.get("TELUS_LLM_MODEL", "")
        or "gpt-oss:120b"
    )

    # Fallback to legacy TELUS_OLLAMA_URL for Mistral endpoint
    if not url:
        url = os.environ.get("TELUS_OLLAMA_URL") or secrets.get("TELUS_OLLAMA_URL", "")
        if url:
            model = "mistralai/Mistral-Small-3.2-24B-Instruct-2506"
    if not key:
        key = os.environ.get("TELUS_API_KEY") or secrets.get("TELUS_API_KEY", "")

    return url, key, model


def summarize(messages: list[dict]) -> tuple[str, str]:
    """Call TELUS LLM to summarize messages. Returns (summary, model_used)."""
    url, key, model = get_config()

    if not url or not key:
        return "(summary unavailable — no TELUS credentials found)", "none"

    # Build conversation text from last 100 messages
    lines = []
    for m in messages[-100:]:
        name = m.get("sender_name") or m.get("sender_id") or "Unknown"
        ts = (m.get("platform_ts") or "")[:16]
        content = m.get("content") or ""
        if content:
            lines.append(f"[{ts}] {name}: {content}")

    if not lines:
        return "(empty thread)", model

    conversation = "\n".join(lines)

    # Truncate to ~8K chars to stay within context limits
    if len(conversation) > 8000:
        conversation = conversation[-8000:]

    resp = requests.post(
        f"{url.rstrip('/')}/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": conversation},
            ],
            "max_tokens": 200,
            "temperature": 0.3,
        },
        timeout=30,
    )
    resp.raise_for_status()
    summary = resp.json()["choices"][0]["message"]["content"].strip()
    return summary, model


def main():
    data = json.load(sys.stdin)
    summary, model_used = summarize(data.get("messages", []))
    json.dump({"summary": summary, "model": model_used}, sys.stdout)


if __name__ == "__main__":
    main()
