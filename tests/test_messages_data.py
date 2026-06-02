from __future__ import annotations

from pathlib import Path

import messages_data as md


def test_list_returns_reverse_chronological(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"limit": 10, "offset": 0})
    ids = [r["id"] for r in rows]
    # newest platform_ts first: m4(11:00) > m2(10:00) > m3(05-20) > m1(05-01)
    assert ids == ["m4", "m2", "m3", "m1"]


def test_list_resolves_scored_sender_name_and_score(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"limit": 10, "offset": 0})
    m2 = next(r for r in rows if r["id"] == "m2")
    assert m2["sender_name"] == "Darren Zal"
    assert round(m2["connectedness"], 3) == 0.854
    assert m2["priority_tier"] == "support_clique"


def test_list_unscored_sender_degrades_gracefully(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"limit": 10, "offset": 0})
    m3 = next(r for r in rows if r["id"] == "m3")
    # No identity/score: name falls back to raw sender_id, score is None
    assert m3["sender_name"] == "signal:user:zzz-unknown"
    assert m3["connectedness"] is None
    assert m3["priority_tier"] is None
    assert m3["tags"] == [] and m3["ventures"] == []


def test_list_platform_filter(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"platform": "signal", "limit": 10})
    assert [r["id"] for r in rows] == ["m3"]


def test_list_direction_filter(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"direction": "sent", "limit": 10})
    assert [r["id"] for r in rows] == ["m4"]


def test_list_date_range_filter(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"since": "2026-05-15T00:00:00Z", "until": "2026-06-01T10:30:00Z", "limit": 10})
    # m2(06-01 10:00) and m3(05-20) fall in range; m1(05-01) and m4(11:00) excluded
    assert sorted(r["id"] for r in rows) == ["m2", "m3"]


def test_list_pagination_offset(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    page1 = md.list_messages(conn, {"limit": 2, "offset": 0})
    page2 = md.list_messages(conn, {"limit": 2, "offset": 2})
    assert [r["id"] for r in page1] == ["m4", "m2"]
    assert [r["id"] for r in page2] == ["m3", "m1"]


def test_connect_ro_is_read_only(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    import sqlite3
    import pytest
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO messages(id,platform,platform_ts,synced_at) VALUES ('x','x','t','t')")


def test_list_populates_thread_fields(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"limit": 10})
    m2 = next(r for r in rows if r["id"] == "m2")
    assert m2["thread_title"] == "Darren ↔ Shawn"
    assert "thread_participants" in m2  # routed from the threads join, not dropped


def test_search_matches_fts_and_orders_reverse_chron(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.search_messages(conn, {"q": "multisig", "limit": 10})
    # m1 + m2 contain "multisig"; reverse-chron → m2 before m1
    assert [r["id"] for r in rows] == ["m2", "m1"]


def test_search_combines_with_filters(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.search_messages(conn, {"q": "multisig", "since": "2026-05-15T00:00:00Z", "limit": 10})
    assert [r["id"] for r in rows] == ["m2"]


def test_search_empty_query_returns_empty(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    assert md.search_messages(conn, {"q": "   ", "limit": 10}) == []


def test_facets_reports_platforms_and_directions(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    f = md.get_facets(conn)
    platforms = {p["value"]: p["count"] for p in f["platforms"]}
    assert platforms == {"telegram": 3, "signal": 1}
    assert set(d["value"] for d in f["directions"]) == {"received", "sent"}
    assert "support_clique" in [d["value"] for d in f["dunbar_layers"]]
