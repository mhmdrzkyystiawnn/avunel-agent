"""Tests for F0-11: Avunel Serve (SSE, Auth, Origin, Cursor, and Shutdown)."""

from __future__ import annotations

import json
from pathlib import Path
import socket
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pytest

from avunel_serve import AvunelEventServer
from hermes_state_events import open_events


def get_free_port() -> int:
    """Find an available port on loopback."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def serve_instance(tmp_path: Path):
    """Spin up an AvunelEventServer on a dynamic free port for isolated testing."""
    events_path = tmp_path / "events.db"
    store = open_events(events_path)
    port = get_free_port()
    token = "test-token-secret-123"

    server = AvunelEventServer(event_store=store, host="127.0.0.1", port=port, token=token)
    server.start(background=True)
    time.sleep(0.1)

    yield server, store, port, token

    server.shutdown()


def test_serve_cockpit_html_unauthenticated(serve_instance) -> None:
    """The root path serves Cockpit HTML without requiring auth (browser landing page)."""
    server, store, port, token = serve_instance
    url = f"http://127.0.0.1:{port}/"

    req = Request(url)
    with urlopen(req) as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Type").startswith("text/html")
        body = resp.read().decode("utf-8")
        assert "Avunel Cockpit" in body


def test_serve_auth_rejection(serve_instance) -> None:
    """Missing or invalid token returns 401 Unauthorized."""
    server, store, port, token = serve_instance
    health_url = f"http://127.0.0.1:{port}/api/health"

    # 1. No token -> 401
    with pytest.raises(HTTPError) as exc_info:
        urlopen(Request(health_url))
    assert exc_info.value.code == 401
    err_body = json.loads(exc_info.value.read().decode("utf-8"))
    assert "Unauthorized" in err_body["error"]

    # 2. Wrong token header -> 401
    bad_req = Request(health_url, headers={"Authorization": "Bearer wrong-token"})
    with pytest.raises(HTTPError) as exc_info:
        urlopen(bad_req)
    assert exc_info.value.code == 401


def test_serve_auth_success_via_header_and_query(serve_instance) -> None:
    """Valid token in Authorization header or query param succeeds."""
    server, store, port, token = serve_instance

    # 1. Authorization Header
    req1 = Request(
        f"http://127.0.0.1:{port}/api/health",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urlopen(req1) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "ok"
        assert data["port"] == port

    # 2. Query Param
    req2 = Request(f"http://127.0.0.1:{port}/api/health?token={token}")
    with urlopen(req2) as resp:
        assert resp.status == 200
        data = json.loads(resp.read().decode("utf-8"))
        assert data["status"] == "ok"


def test_serve_origin_rejection(serve_instance) -> None:
    """Untrusted Origin header returns 403 Forbidden."""
    server, store, port, token = serve_instance
    url = f"http://127.0.0.1:{port}/api/health?token={token}"

    # 1. Disallowed external origin
    bad_origin_req = Request(url, headers={"Origin": "http://evil.com"})
    with pytest.raises(HTTPError) as exc_info:
        urlopen(bad_origin_req)
    assert exc_info.value.code == 403
    err_body = json.loads(exc_info.value.read().decode("utf-8"))
    assert "Forbidden" in err_body["error"]

    # 2. Allowed loopback origin
    good_origin_req = Request(url, headers={"Origin": f"http://127.0.0.1:{port}"})
    with urlopen(good_origin_req) as resp:
        assert resp.status == 200


def test_serve_sse_cursor_resume(serve_instance) -> None:
    """SSE stream streams events after last_id without duplicating earlier events."""
    server, store, port, token = serve_instance

    # Emit 4 events
    for i in range(1, 5):
        store.emit(
            type="message.appended",
            ts=int(time.time() * 1000),
            session_id="sess_cursor_test",
            actor="cli:test",
            payload={
                "session_id": "sess_cursor_test",
                "message_uid": f"uid_{i}",
                "role": "user",
                "content": f"Message {i}",
                "timestamp": float(i),
            },
            api_content_ref=f"msg:uid_{i}",
        )

    # Stream starting from last_id=2 (should get events 3 and 4)
    url = f"http://127.0.0.1:{port}/api/events/stream?token={token}&last_id=2"
    req = Request(url)

    with urlopen(req) as resp:
        assert resp.status == 200
        assert resp.headers.get("Content-Type").startswith("text/event-stream")

        received_events = []
        # Read two events from stream using readline for immediate responsiveness
        while len(received_events) < 2:
            line = resp.readline().decode("utf-8")
            if not line:
                break
            line = line.strip()
            if line.startswith("data: "):
                data = json.loads(line[6:].strip())
                received_events.append(data)

def test_server_refuses_start_without_token(tmp_path: Path, monkeypatch) -> None:
    """Server must refuse to initialize without a mandatory token."""
    monkeypatch.delenv("AVUNEL_SERVE_TOKEN", raising=False)
    store = open_events(tmp_path / "events.db")

    with pytest.raises(ValueError, match="Auth token is mandatory"):
        AvunelEventServer(event_store=store, host="127.0.0.1", port=31032, token=None)

    with pytest.raises(ValueError, match="Auth token is mandatory"):
        AvunelEventServer(event_store=store, host="127.0.0.1", port=31032, token="")


def test_cockpit_html_contains_named_event_listeners(serve_instance) -> None:
    """Cockpit web client must attach addEventListener for named SSE event types."""
    server, store, port, token = serve_instance
    url = f"http://127.0.0.1:{port}/"

    with urlopen(Request(url)) as resp:
        body = resp.read().decode("utf-8")

        # Must attach addEventListener for named event types per Contract §2
        assert "addEventListener('message.appended'" in body or 'addEventListener(t, handleIncomingEvent)' in body
        assert "message.appended" in body
        assert "session.created" in body
        assert "blob.stored" in body
        assert "ev.id <= lastId" in body  # Deduplication check
        assert "connectSSE" in body


def test_web_client_sse_named_events_and_metadata_payload(serve_instance) -> None:
    """Web SSE stream must deliver named events with is_redacted and has_blob booleans."""
    server, store, port, token = serve_instance

    # Emit an event with secret and CAS blob
    store.emit(
        type="message.appended",
        ts=int(time.time() * 1000),
        session_id="sess_web_test",
        actor="cli:web",
        payload={
            "session_id": "sess_web_test",
            "message_uid": "uid_web_001",
            "role": "user",
            "content": "Secret sk-live-11112222333344445555 should be masked",
            "content_blob_ref": "sha256:abc987654321",
        },
        api_content_ref="msg:uid_web_001",
    )

    url = f"http://127.0.0.1:{port}/api/events/stream?token={token}&last_id=0"
    with urlopen(Request(url)) as resp:
        lines = []
        for _ in range(10):
            line = resp.readline().decode("utf-8").strip()
            if not line:
                if lines:
                    break
                continue
            lines.append(line)

        assert any(l.startswith("event: message.appended") for l in lines)
        data_line = [l for l in lines if l.startswith("data: ")][0]
        data = json.loads(data_line[6:])

        assert data["type"] == "message.appended"
        assert data["is_redacted"] is True
        assert data["has_blob"] is True
        assert "sk-liv...5555" in data["payload_json"] or "sk-liv..." in data["payload_json"]


def test_web_reconnect_last_event_id_header_and_deduplication(serve_instance) -> None:
    """Web reconnect must prioritize Last-Event-ID header and prevent event duplicates."""
    server, store, port, token = serve_instance

    # Emit 5 events
    for i in range(1, 6):
        store.emit(
            type="message.appended",
            ts=int(time.time() * 1000),
            session_id="sess_reconnect",
            actor="cli:web",
            payload={
                "session_id": "sess_reconnect",
                "message_uid": f"uid_rec_{i}",
                "role": "user",
                "content": f"Rec Message {i}",
            },
            api_content_ref=f"msg:uid_rec_{i}",
        )

    # 1. First connection: read events 1 and 2
    url_initial = f"http://127.0.0.1:{port}/api/events/stream?token={token}&last_id=0"
    first_batch = []
    with urlopen(Request(url_initial)) as resp:
        while len(first_batch) < 2:
            line = resp.readline().decode("utf-8").strip()
            if line.startswith("data: "):
                first_batch.append(json.loads(line[6:]))

    assert len(first_batch) == 2
    assert first_batch[0]["id"] == 1
    assert first_batch[1]["id"] == 2
    last_seen_id = first_batch[1]["id"]

    # 2. Reconnect: Browser sends Last-Event-ID header even if URL still has ?last_id=0
    url_reconnect = f"http://127.0.0.1:{port}/api/events/stream?token={token}&last_id=0"
    req_reconnect = Request(
        url_reconnect,
        headers={"Last-Event-ID": str(last_seen_id)},
    )

    reconnected_batch = []
    with urlopen(req_reconnect) as resp:
        while len(reconnected_batch) < 3:
            line = resp.readline().decode("utf-8").strip()
            if line.startswith("data: "):
                reconnected_batch.append(json.loads(line[6:]))

    # Reconnect stream must only receive IDs 3, 4, 5 (zero duplicate of 1 or 2)
    assert len(reconnected_batch) == 3
    received_ids = [e["id"] for e in reconnected_batch]
    assert received_ids == [3, 4, 5]
    assert 1 not in received_ids
    assert 2 not in received_ids
