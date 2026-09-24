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


def test_detail_returns_full_record_with_reply_context(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    d = md.get_message_detail(conn, "m2")
    assert d["id"] == "m2"
    assert d["sender_name"] == "Darren Zal"
    assert d["content"] == "lets align on the multisig split"
    assert d["thread_title"] == "Darren ↔ Shawn"


def test_detail_missing_returns_none(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    assert md.get_message_detail(conn, "nope") is None


def test_stats_reports_total_and_platform_breakdown(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    s = md.get_stats(conn)
    assert s["total_messages"] == 4
    assert s["by_platform"]["telegram"] == 3
    assert s["total_threads"] == 2


def test_signature_changes_when_file_changes(fixture_db: Path, tmp_path) -> None:
    sig1 = md.signature(fixture_db)
    # append a row via a separate RW connection to simulate a sync
    import sqlite3
    rw = sqlite3.connect(str(fixture_db))
    rw.execute(
        "INSERT INTO messages(id,platform,thread_id,sender_id,content,platform_ts,synced_at,direction) "
        "VALUES ('m5','telegram','t1','telegram:user:000-me','later','2026-06-02T00:00:00Z','2026-06-02T00:00:00Z','sent')"
    )
    rw.commit(); rw.close()
    sig2 = md.signature(fixture_db)
    assert sig1 != sig2


def test_engaged_thread_ids(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    engaged = md.engaged_thread_ids(conn)
    assert "t1" in engaged   # m4 is direction='sent' in t1
    assert "t2" not in engaged


def test_card_has_thread_type(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"limit": 10})
    m2 = next(r for r in rows if r["id"] == "m2")
    assert m2["thread_type"] == "dm"


def test_list_threads_recency_order_and_enrichment(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_threads(conn, {"limit": 10})
    assert [r["id"] for r in rows] == ["t1", "t2"]   # updated_at desc
    t1 = rows[0]
    assert t1["title"] == "Darren ↔ Shawn"
    assert t1["message_count"] == 3
    assert t1["last_content"] == "ok sounds good"   # m4 newest


def test_list_threads_platform_filter(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_threads(conn, {"platform": "signal", "limit": 10})
    assert [r["id"] for r in rows] == ["t2"]


def test_get_thread_returns_timeline(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    t = md.get_thread(conn, "t1")
    assert t["title"] == "Darren ↔ Shawn"
    ids = [m["id"] for m in t["messages"]]
    assert ids == ["m4", "m2", "m1"]   # reverse-chron within thread
    assert md.get_thread(conn, "nope") is None


def test_filters_thread_id(fixture_db: Path) -> None:
    conn = md.connect_ro(fixture_db)
    rows = md.list_messages(conn, {"thread": "t2", "limit": 10})
    assert [r["id"] for r in rows] == ["m3"]
