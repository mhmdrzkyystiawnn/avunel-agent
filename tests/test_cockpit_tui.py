"""Tests for Avunel Timeline TUI & Cockpit Client (F0-12)."""

from __future__ import annotations

import io
import json
from pathlib import Path
import threading
import time
from typing import Generator, Tuple

import pytest

from avunel_serve import AvunelEventServer
from avunel_timeline_tui import (
    COLOR_BOLD,
    COLOR_RESET,
    TimelineTUIClient,
    format_ts,
)
from f0_contract import (
    DEFAULT_HOST,
    EVENT_TYPE_BLOB_STORED,
    EVENT_TYPE_MESSAGE_APPENDED,
    EVENT_TYPE_SESSION_CREATED,
    EVENT_TYPE_SESSION_REWOUND,
)
from hermes_state_events import EventStore


@pytest.fixture
def tui_server(tmp_path: Path) -> Generator[Tuple[AvunelEventServer, EventStore, int, str], None, None]:
    events_db = tmp_path / "events.db"
    store = EventStore(events_db)
    token = "tui-test-token-xyz"

    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((DEFAULT_HOST, 0))
        port = s.getsockname()[1]

    server = AvunelEventServer(
        event_store=store,
        host=DEFAULT_HOST,
        port=port,
        token=token,
    )
    server.start(background=True)
    time.sleep(0.05)

    try:
        yield server, store, port, token
    finally:
        server.shutdown()


def test_tui_client_requires_token(monkeypatch) -> None:
    """TimelineTUIClient must refuse to stream without a token."""
    monkeypatch.delenv("AVUNEL_SERVE_TOKEN", raising=False)
    client = TimelineTUIClient(token=None)
    with pytest.raises(ValueError, match="Auth token is mandatory"):
        list(client.stream_events(timeout=0.1))


def test_tui_event_formatting() -> None:
    """Test format_event with and without ANSI color."""
    client_color = TimelineTUIClient(token="test-token", color=True)
    client_no_color = TimelineTUIClient(token="test-token", color=False)

    sample_event = {
        "id": 42,
        "ts": 1700000000000,
        "type": EVENT_TYPE_MESSAGE_APPENDED,
        "session_id": "sess_1234567890ab",
        "actor": "user:alice",
        "payload": {
            "role": "user",
            "content": "Halo Avunel, tolong jelaskan arsitektur event log.",
        },
    }

    formatted_color = client_color.format_event(sample_event)
    formatted_no_color = client_no_color.format_event(sample_event)

    assert "#42" in formatted_no_color
    assert "[MSG]" in formatted_no_color
    assert "<user>" in formatted_no_color
    assert "Halo Avunel" in formatted_no_color
    assert COLOR_RESET not in formatted_no_color

    assert COLOR_RESET in formatted_color
    assert "#42" in formatted_color


def test_tui_client_stream_and_filter(tui_server) -> None:
    """Test TimelineTUIClient streaming events with session and type filters."""
    server, store, port, token = tui_server

    # Emit test events
    store.emit(
        type=EVENT_TYPE_SESSION_CREATED,
        ts=int(time.time() * 1000),
        session_id="sess_alpha",
        actor="cli:test",
        payload={"session_id": "sess_alpha", "title": "Alpha Session"},
    )
    store.emit(
        type=EVENT_TYPE_MESSAGE_APPENDED,
        ts=int(time.time() * 1000),
        session_id="sess_alpha",
        actor="user:bob",
        payload={"message_uid": "msg_alpha_1", "role": "user", "content": "Alpha message 1"},
        api_content_ref="msg:msg_alpha_1",
    )
    store.emit(
        type=EVENT_TYPE_MESSAGE_APPENDED,
        ts=int(time.time() * 1000),
        session_id="sess_beta",
        actor="user:charlie",
        payload={"message_uid": "msg_beta_1", "role": "user", "content": "Beta message 1"},
        api_content_ref="msg:msg_beta_1",
    )
    store.emit(
        type=EVENT_TYPE_BLOB_STORED,
        ts=int(time.time() * 1000),
        session_id="sess_alpha",
        actor="tool:store",
        payload={"digest": "abcdef1234567890", "byte_size": 2048, "mime_type": "text/plain"},
    )

    out_buf = io.StringIO()
    client = TimelineTUIClient(
        host=DEFAULT_HOST,
        port=port,
        token=token,
        session_id="sess_alpha",
        color=False,
        out=out_buf,
    )

    # Collect up to 3 events matching sess_alpha
    events = list(client.stream_events(max_events=3, timeout=3.0))

    assert len(events) == 3
    assert all(e["session_id"] == "sess_alpha" for e in events)
    types = [e["type"] for e in events]
    assert types == [
        EVENT_TYPE_SESSION_CREATED,
        EVENT_TYPE_MESSAGE_APPENDED,
        EVENT_TYPE_BLOB_STORED,
    ]


def test_tui_cursor_resume(tui_server) -> None:
    """Test TimelineTUIClient starting with last_id > 0."""
    server, store, port, token = tui_server

    for i in range(1, 6):
        store.emit(
            type=EVENT_TYPE_MESSAGE_APPENDED,
            ts=int(time.time() * 1000),
            session_id="sess_cursor",
            actor="cli:test",
            payload={"message_uid": f"msg_cursor_{i}", "role": "user", "content": f"Msg {i}"},
            api_content_ref=f"msg:msg_cursor_{i}",
        )

    out_buf = io.StringIO()
    client = TimelineTUIClient(
        host=DEFAULT_HOST,
        port=port,
        token=token,
        last_id=3,
        color=False,
        out=out_buf,
    )

    # Should receive event 4 and 5
    events = list(client.stream_events(max_events=2, timeout=3.0))
    assert len(events) == 2
    assert events[0]["id"] == 4
    assert events[1]["id"] == 5
    assert client.last_id == 5


def test_tui_run_bounded(tui_server) -> None:
    """Test client.run(...) with bounded count and verify stdout output."""
    server, store, port, token = tui_server

    store.emit(
        type=EVENT_TYPE_SESSION_REWOUND,
        ts=int(time.time() * 1000),
        session_id="sess_rewind",
        actor="cli:test",
        payload={"rewound_count": 2, "target_message_uid": "msg_xyz"},
    )

    out_buf = io.StringIO()
    client = TimelineTUIClient(
        host=DEFAULT_HOST,
        port=port,
        token=token,
        color=False,
        out=out_buf,
    )

    count = client.run(max_events=1, timeout=3.0)
    assert count == 1
    output = out_buf.getvalue()
    assert "=== Avunel Cockpit Timeline" in output
    assert "[UNDO]" in output
    assert "Undone 2 messages from msg_xyz" in output
