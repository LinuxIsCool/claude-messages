from __future__ import annotations

from pathlib import Path

from claude_webui.accessor import Accessor
from messages_accessor import MessagesAccessor


def test_accessor_satisfies_protocol(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    assert isinstance(acc, Accessor)


def test_list_delegates_reverse_chron(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.list({"limit": 10})
    assert [r["id"] for r in rows] == ["m4", "m2", "m3", "m1"]


def test_list_coerces_string_limit(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.list({"limit": "2"})  # query args arrive as strings
    assert len(rows) == 2


def test_search_via_q_param(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.search({"q": "multisig", "limit": 10})
    assert [r["id"] for r in rows] == ["m2", "m1"]


def test_detail_and_missing(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    assert acc.detail("m2")["content"] == "lets align on the multisig split"
    assert acc.detail("nope") == {"error": "not found", "id": "nope"}


def test_stats_and_facets(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    assert acc.stats()["total_messages"] == 4
    assert {p["value"] for p in acc.facets()["platforms"]} == {"telegram", "signal"}


def test_healthz_ok(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    h = acc.healthz()
    assert h["ok"] is True
    assert h["namespace"] == "legion.claude-message"


def test_feed_is_reverse_chron(fixture_db: Path) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.feed({"limit": 2})
    assert [r["id"] for r in rows] == ["m4", "m2"]
