from __future__ import annotations

import salience as sal


def _msg(**kw):
    base = dict(
        id="m", thread_id="t1", sender_id="telegram:user:1",
        sender_name="telegram:user:1", content="hello", thread_type="group",
        connectedness=None, priority_tier=None,
    )
    base.update(kw)
    return base


def test_high_signal_dm_from_scored_human_in_engaged_thread() -> None:
    ctx = {"engaged_threads": {"t1"}}
    msg = _msg(thread_type="dm", sender_name="Darren Zal", connectedness=0.85,
               priority_tier="support_clique", content="lets align on the plan")
    out = sal.salience(msg, ctx)
    assert out["salience"] >= 0.8
    assert out["reasons"][0]["signal"] in sal.SIGNALS


def test_low_signal_broadcast_spam_unscored_handle_never_replied() -> None:
    ctx = {"engaged_threads": set()}
    msg = _msg(thread_type="channel", sender_name="telegram:user:999",
               sender_id="telegram:user:999", connectedness=None,
               content="🔥🔥🔥🔥🔥 BUY NOW https://scam.io https://x.io 💰💰💰")
    out = sal.salience(msg, ctx)
    assert out["salience"] <= 0.35


def test_reciprocity_dominates_for_engaged_vs_not() -> None:
    base = dict(thread_type="group", sender_name="X", sender_id="y", connectedness=0.4)
    hi = sal.salience(_msg(**base), {"engaged_threads": {"t1"}})["salience"]
    lo = sal.salience(_msg(**base), {"engaged_threads": set()})["salience"]
    assert hi > lo


def test_breakdown_is_transparent_and_sorted() -> None:
    out = sal.salience(_msg(), {"engaged_threads": set()})
    assert isinstance(out["reasons"], list) and out["reasons"]
    contribs = [r["contribution"] for r in out["reasons"]]
    assert contribs == sorted(contribs, reverse=True)
    for r in out["reasons"]:
        assert {"signal", "value", "weight", "contribution", "reason"} <= set(r)


def test_salience_is_bounded_0_1() -> None:
    for tt in ("dm", "channel", "group", None):
        out = sal.salience(_msg(thread_type=tt), {"engaged_threads": {"t1"}})
        assert 0.0 <= out["salience"] <= 1.0


def test_registry_is_extensible_new_signal_affects_score() -> None:
    # Evergreen contract: a new signal registers and participates without
    # touching the blender.
    @sal.signal("unit_test_boost")
    def _boost(msg, ctx):
        return 1.0, "test boost"
    try:
        w = dict(sal.DEFAULT_WEIGHTS); w["unit_test_boost"] = 1.0
        out = sal.salience(_msg(), {"engaged_threads": set()}, weights=w)
        assert any(r["signal"] == "unit_test_boost" for r in out["reasons"])
    finally:
        sal.SIGNALS.pop("unit_test_boost", None)


def test_content_quality_penalizes_links_and_emoji() -> None:
    clean, _ = sal.SIGNALS["content_quality"](_msg(content="hey can we talk tomorrow"), {})
    noisy, _ = sal.SIGNALS["content_quality"](
        _msg(content="🔥🔥🔥🔥 https://a.io https://b.io 💰💰"), {})
    assert clean > noisy
