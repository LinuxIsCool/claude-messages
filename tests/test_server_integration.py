"""Boot the real kernel in a thread on an ephemeral port against the fixture
DB and exercise the HTTP surface end-to-end."""
from __future__ import annotations

import json
import socket
import threading
import time
import urllib.request
from pathlib import Path

import pytest

import server as srv


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@pytest.fixture()
def live_server(fixture_db: Path, monkeypatch):
    import messages_data as md
    monkeypatch.setattr(md, "DEFAULT_DB_PATH", fixture_db)
    port = _free_port()
    kernel = srv.build_kernel(port=port, bind="127.0.0.1")
    t = threading.Thread(target=kernel.serve, daemon=True)
    t.start()
    # wait for bind
    for _ in range(50):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=0.5)
            break
        except Exception:
            time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"


def _get(url: str):
    with urllib.request.urlopen(url, timeout=2) as r:
        return r.status, r.read().decode()


def test_healthz(live_server: str) -> None:
    status, body = _get(f"{live_server}/healthz")
    assert status == 200
    assert json.loads(body)["ok"] is True


def test_root_serves_spa(live_server: str) -> None:
    status, body = _get(f"{live_server}/")
    assert status == 200
    assert 'id="grid"' in body  # the card grid container


def test_api_list_reverse_chron(live_server: str) -> None:
    status, body = _get(f"{live_server}/api/list?limit=10")
    assert status == 200
    ids = [r["id"] for r in json.loads(body)]
    assert ids == ["m4", "m2", "m3", "m1"]


def test_api_search(live_server: str) -> None:
    status, body = _get(f"{live_server}/api/search?q=multisig&limit=10")
    assert status == 200
    assert [r["id"] for r in json.loads(body)] == ["m2", "m1"]


def test_api_facets(live_server: str) -> None:
    status, body = _get(f"{live_server}/api/facets")
    assert status == 200
    assert {p["value"] for p in json.loads(body)["platforms"]} == {"telegram", "signal"}


def test_mutation_verb_405(live_server: str) -> None:
    req = urllib.request.Request(f"{live_server}/api/list", method="POST", data=b"{}")
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=2)
    assert ei.value.code == 405
