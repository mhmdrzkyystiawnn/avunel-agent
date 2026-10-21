"""Tests for events_fts.py (F0-09).

Verifies:
- FTS5 virtual table events_fts setup and AFTER INSERT trigger sync.
- Full-text search over payload_json, type, and session_id with ranking.
- Session and type scoping filters.
- Redaction synchronization: redactions overlay purges old secrets from FTS.
- Narrowed G0b for blob messages: search operates over <field>_snippet (up to 2 KiB).
- FTS-scoped corruption detection and recovery.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import pytest

from events_fts import (
    ensure_events_fts,
    rebuild_events_fts,
    is_events_fts_corruption_error,
    repair_events_fts_if_corrupt,
    search_events,
)
from hermes_state_events import EventStore, open_events
from blob_store import BlobStore, offload_payload_if_needed


def _fake_ghp(suffix: str = "123456789012345678901234567890123456") -> str:
    return "".join(["g", "h", "p", "_", suffix])


def _fake_sk(suffix: str = "1234567890abcdef1234567890") -> str:
    return "".join(["s", "k", "-", suffix])


@pytest.fixture
def event_store(tmp_path: Path) -> EventStore:
    store = open_events(tmp_path / "events.db")
    with store.acquire() as conn:
        ensure_events_fts(conn)
    return store


def test_events_fts_auto_indexing_on_emit(event_store: EventStore) -> None:
    """AFTER INSERT trigger on events automatically synchronizes new events to events_fts."""
    e1 = event_store.emit(
        "message.user",
        ts=1,
        session_id="sess_fts_1",
        payload={"message_uid": "u1", "content": "The quick brown fox jumps over the lazy dog"},
        api_content_ref="msg:u1",
    )
    e2 = event_store.emit(
        "message.assistant",
        ts=2,
        session_id="sess_fts_1",
        payload={"message_uid": "u2", "content": "Avunel agent architecture and event log"},
        api_content_ref="msg:u2",
    )

    with event_store.acquire() as conn:
        # Search for fox
        res = search_events(conn, "fox")
        assert len(res) == 1
        assert res[0]["id"] == e1

        # Search for architecture
        res2 = search_events(conn, "architecture")
        assert len(res2) == 1
        assert res2[0]["id"] == e2

        # Search for non-existent word
        assert len(search_events(conn, "kangaroo")) == 0


def test_events_fts_scoping_by_session_and_type(event_store: EventStore) -> None:
    """search_events respects session_id and event_type filters."""
    event_store.emit(
        "message.user",
        ts=1,
        session_id="sess_A",
        payload={"message_uid": "ua1", "content": "Common topic discussed here"},
        api_content_ref="msg:ua1",
    )
    event_store.emit(
        "message.user",
        ts=2,
        session_id="sess_B",
        payload={"message_uid": "ub1", "content": "Common topic discussed elsewhere"},
        api_content_ref="msg:ub1",
    )
    event_store.emit(
        "session.created",
        ts=3,
        session_id="sess_A",
        payload={"source": "cli", "topic": "Common metadata"},
    )

    with event_store.acquire() as conn:
        # Without filter: finds all 3
        all_hits = search_events(conn, "Common")
        assert len(all_hits) == 3

        # Scoped to sess_A: finds 2
        hits_sess_a = search_events(conn, "Common", session_id="sess_A")
        assert len(hits_sess_a) == 2
        assert all(h["session_id"] == "sess_A" for h in hits_sess_a)

        # Scoped to sess_A and type message.user: finds 1
        hits_typed = search_events(conn, "Common", session_id="sess_A", event_type="message.user")
        assert len(hits_typed) == 1
        assert hits_typed[0]["type"] == "message.user"


def test_events_fts_redaction_sync(event_store: EventStore) -> None:
    """Redacting an event purges old secrets from events_fts and indexes clean text."""
    secret = _fake_ghp()
    eid = event_store.emit(
        "message.user",
        ts=1,
        session_id="sess_leak",
        payload={"message_uid": "uleak", "content": f"Confidential token: {secret}"},
        api_content_ref="msg:uleak",
        redact=False,  # Unredacted pre-write to simulate leak
    )

    with event_store.acquire() as conn:
        # Pre-redaction: secret matches
        before = search_events(conn, f'"{secret}"')
        assert len(before) == 1
        assert before[0]["id"] == eid

    # Perform redaction
    event_store.redact(eid, reason="leaked secret token")

    with event_store.acquire() as conn:
        # Post-redaction: secret MUST NOT match
        after_secret = search_events(conn, f'"{secret}"')
        assert len(after_secret) == 0

        # Masked text or surrounding words match
        after_word = search_events(conn, "Confidential")
        assert len(after_word) == 1
        assert after_word[0]["id"] == eid


def test_events_fts_narrowed_g0b_blob_snippet_search(tmp_path: Path, event_store: EventStore) -> None:
    """Narrowed G0b: for blob-offloaded messages, FTS matches words in 2 KiB snippet, but not beyond."""
    blob_store = BlobStore(tmp_path / "blobs")

    # Construct payload > 60 KiB:
    # First 500 bytes: includes "SnippetKeyword"
    # After 3000 bytes (beyond 2 KiB snippet): includes "DeepInsideBlobKeyword"
    head = "This is the start of the giant payload with SnippetKeyword. " + ("x" * 1500)
    tail = ("y" * 60000) + " and DeepInsideBlobKeyword at the very end."
    giant_content = head + " " + tail

    raw_payload = {
        "message_uid": "ublob1",
        "role": "assistant",
        "content": giant_content,
    }
    offloaded = offload_payload_if_needed(raw_payload, blob_store=blob_store, threshold_bytes=1000)
    assert offloaded["content"] is None
    assert "content_snippet" in offloaded
    assert "content_blob_ref" in offloaded

    eid = event_store.emit(
        "message.appended",
        ts=1,
        session_id="sess_blob",
        payload=offloaded,
        api_content_ref="msg:ublob1",
    )

    with event_store.acquire() as conn:
        # Term inside snippet is found
        hits_snippet = search_events(conn, "SnippetKeyword")
        assert len(hits_snippet) == 1
        assert hits_snippet[0]["id"] == eid

        # Term outside 2 KiB snippet is NOT in FTS index (narrowed G0b contract)
        hits_deep = search_events(conn, "DeepInsideBlobKeyword")
        assert len(hits_deep) == 0


def test_events_fts_corruption_detection_and_rebuild(event_store: EventStore) -> None:
    """is_events_fts_corruption_error detects corruption and repair_events_fts_if_corrupt recovers."""
    # Test corruption marker detection
    err = sqlite3.DatabaseError("database disk image is malformed")
    assert is_events_fts_corruption_error(err) is True
    assert is_events_fts_corruption_error(RuntimeError("unrelated failure")) is False

    # Insert event
    event_store.emit(
        "message.user",
        ts=1,
        session_id="sess_c",
        payload={"message_uid": "uc1", "content": "Recovery test text"},
        api_content_ref="msg:uc1",
    )

    with event_store.acquire() as conn:
        # Rebuild succeeds
        rebuild_events_fts(conn)
        hits = search_events(conn, "Recovery")
        assert len(hits) == 1

        # Simulate repair on corruption error
        assert repair_events_fts_if_corrupt(conn, err) is True
        hits_after = search_events(conn, "Recovery")
        assert len(hits_after) == 1
