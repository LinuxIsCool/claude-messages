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


def test_list_annotates_salience(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.list({"limit": 10})
    for r in rows:
        assert 0.0 <= r["salience"] <= 1.0
        assert isinstance(r["salience_reasons"], list) and r["salience_reasons"]


def test_signal_sort_ranks_high_salience_first(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.list({"sort": "signal", "limit": 10})
    sals = [r["salience"] for r in rows]
    assert sals == sorted(sals, reverse=True)
    # m2 (Darren, dm, engaged, scored) must outrank m3 (group, never-replied, unscored)
    ids = [r["id"] for r in rows]
    assert ids.index("m2") < ids.index("m3")


def test_hide_noise_drops_low_salience(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    full = acc.list({"limit": 10})
    hidden = acc.list({"limit": 10, "hide_noise": "true"})
    assert len(hidden) <= len(full)
    assert all(r["salience"] >= 0.35 for r in hidden)


def test_detail_includes_salience(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    d = acc.detail("m2")
    assert "salience" in d and d["salience_reasons"]


def test_threads_accessor_ranks_and_annotates(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    rows = acc.threads({"limit": 10})
    assert [r["id"] for r in rows] == ["t1", "t2"]
    for r in rows:
        assert "salience" in r and isinstance(r["salience_reasons"], list)


def test_thread_detail_accessor(fixture_db) -> None:
    acc = MessagesAccessor(db_path=fixture_db)
    t = acc.thread("t1")
    assert t["title"] == "Darren ↔ Shawn"
    assert [m["id"] for m in t["messages"]] == ["m4", "m2", "m1"]
    assert all("salience" in m for m in t["messages"])
    assert acc.thread("nope") == {"error": "not found", "id": "nope"}
