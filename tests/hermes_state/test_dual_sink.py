"""Tests for DualSink (F0-06).

Verifies:
- Dual-write parity: state.db matches events.db projection replay (except redaction and api_content).
- Legacy mode: AVUNEL_STATE_MODE=legacy bypasses event_store.
- Message data payload: excludes compression_lock_holder, turn_lease_*, ttl.
- Undo / rewind: emits conversation.undone and preserves active state during replay.
- Idempotency: replay using (session_id, message_uid) without active filter avoids duplicate rows.
- Failure injection (Opsi B): state.db failure enters degraded mode, allows replay_pending() recovery.
- Failure injection: event_store.emit failure aborts state.db write (fail-fast).
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import pytest

from dual_sink import DualSink
from hermes_state import SessionDB
import hermes_state_events
from hermes_state_events import EventStore


def _fake_sk(suffix: str = "1234567890abcdef1234567890abcdef") -> str:
    """Construct fake secret key prefix at runtime so static secret scanners find 0 matches."""
    return "".join(["s", "k", "-", suffix])


@pytest.fixture
def tmp_session_db(tmp_path: Path) -> SessionDB:
    return SessionDB(db_path=tmp_path / "state.db")


@pytest.fixture
def tmp_event_store(tmp_path: Path) -> EventStore:
    return hermes_state_events.open_events(tmp_path / "events.db")


def test_legacy_mode_bypasses_event_store(tmp_session_db: SessionDB, tmp_event_store: EventStore) -> None:
    """When mode is 'legacy', events.db is not touched."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="legacy")
    assert sink.mode == "legacy"

    s_id = sink.create_session("sess_legacy", source="cli", model="gpt-4o")
    assert s_id == "sess_legacy"

    msg_id = sink.append_message("sess_legacy", role="user", content="Hello in legacy")
    assert isinstance(msg_id, int)

    title_ok = sink.set_session_title("sess_legacy", "Legacy Title")
    assert title_ok is True

    # event_store must have 0 events
    assert tmp_event_store.count() == 0

    # state.db has the session and message
    assert tmp_session_db.get_session_title("sess_legacy") == "Legacy Title"
    assert tmp_session_db.message_count("sess_legacy") == 1
    sink.close()


def test_dual_write_session_and_message_parity(
    tmp_path: Path, tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """Parity contract: state.db equals replay of events.db, EXCEPT redacted portions and api_content."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    assert sink.mode == "dual"

    sink.create_session("sess_parity", source="cli", model="gpt-4o")
    sink.set_session_title("sess_parity", "Parity Title")

    # Message with an API key: event_store will redact before writing to events.db
    msg1_id = sink.append_message(
        "sess_parity",
        role="user",
        content=f"My token is {_fake_sk()}",
    )
    assert isinstance(msg1_id, int)

    msg2_id = sink.append_message(
        "sess_parity",
        role="assistant",
        content="I will keep it safe",
        token_count=15,
        finish_reason="stop",
    )
    assert isinstance(msg2_id, int)

    # Replay all events into a fresh state2.db
    state2 = SessionDB(db_path=tmp_path / "state2.db")
    projected_count = sink.replay_session("sess_parity", session_db=state2)
    assert projected_count == 4  # 1 session.created + 1 title_updated + 2 message.appended

    # Compare sessions table
    s1 = tmp_session_db._conn.execute("SELECT id, source, model, title FROM sessions WHERE id = 'sess_parity'").fetchone()
    s2 = state2._conn.execute("SELECT id, source, model, title FROM sessions WHERE id = 'sess_parity'").fetchone()
    assert dict(s1) == dict(s2)

    # Compare messages table
    m1_rows = [dict(r) for r in tmp_session_db._conn.execute(
        "SELECT role, content, message_uid, token_count, finish_reason, active FROM messages WHERE session_id = 'sess_parity' ORDER BY id ASC"
    ).fetchall()]
    m2_rows = [dict(r) for r in state2._conn.execute(
        "SELECT role, content, message_uid, token_count, finish_reason, active FROM messages WHERE session_id = 'sess_parity' ORDER BY id ASC"
    ).fetchall()]

    assert len(m1_rows) == 2
    assert len(m2_rows) == 2

    # UIDs and structure match exactly
    assert m1_rows[0]["message_uid"] == m2_rows[0]["message_uid"]
    assert m1_rows[1]["message_uid"] == m2_rows[1]["message_uid"]
    assert m1_rows[1]["token_count"] == m2_rows[1]["token_count"] == 15
    assert m1_rows[1]["finish_reason"] == m2_rows[1]["finish_reason"] == "stop"

    # Parity exception verified: state.db has raw secret, state2 (replayed from events.db) has redacted token
    assert _fake_sk() in m1_rows[0]["content"]
    assert _fake_sk() not in m2_rows[0]["content"]
    assert "".join(["s", "k", "-", "123...cdef"]) in m2_rows[0]["content"]

    sink.close()
    state2.close()


def test_dual_write_undo_emits_conversation_undone_and_replays_identically(
    tmp_path: Path, tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """rewind_to_message emits conversation.undone and replays to the same active state."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_undo", source="cli")

    id1 = sink.append_message("sess_undo", role="user", content="Turn 1")
    id2 = sink.append_message("sess_undo", role="assistant", content="Reply 1")
    id3 = sink.append_message("sess_undo", role="user", content="Turn 2")
    id4 = sink.append_message("sess_undo", role="assistant", content="Reply 2")

    # Rewind to message 3 (user turn 2)
    outcome = sink.rewind_to_message("sess_undo", target_message_id=id3)
    assert outcome["rewound_count"] == 2

    # Verify conversation.undone event was emitted
    events = tmp_event_store.session_events("sess_undo")
    undo_event = next(e for e in events if e["type"] == "conversation.undone")
    assert undo_event["payload"]["target_message_id"] == id3
    assert undo_event["payload"]["count"] == 2
    assert len(undo_event["payload"]["undone_message_uids"]) == 2

    # Replay into fresh state2.db
    state2 = SessionDB(db_path=tmp_path / "state2.db")
    sink.replay_session("sess_undo", session_db=state2)

    rows1 = tmp_session_db._conn.execute("SELECT id, message_uid, active FROM messages WHERE session_id = 'sess_undo' ORDER BY id").fetchall()
    rows2 = state2._conn.execute("SELECT id, message_uid, active FROM messages WHERE session_id = 'sess_undo' ORDER BY id").fetchall()

    actives1 = [r["active"] for r in rows1]
    actives2 = [r["active"] for r in rows2]
    assert actives1 == actives2 == [1, 1, 0, 0]

    uids1 = [r["message_uid"] for r in rows1]
    uids2 = [r["message_uid"] for r in rows2]
    assert uids1 == uids2

    # Check rewind_count
    rw1 = tmp_session_db._conn.execute("SELECT rewind_count FROM sessions WHERE id = 'sess_undo'").fetchone()[0]
    rw2 = state2._conn.execute("SELECT rewind_count FROM sessions WHERE id = 'sess_undo'").fetchone()[0]
    assert rw1 == rw2 == 1

    sink.close()
    state2.close()


def test_rewind_user_turn_intercepted_and_emits_event(
    tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """rewind_user_turn on DualSink emits conversation.undone."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_turn", source="cli")
    sink.append_message("sess_turn", role="user", content="Turn 1")
    sink.append_message("sess_turn", role="assistant", content="Reply 1")
    sink.append_message("sess_turn", role="user", content="Turn 2")
    sink.append_message("sess_turn", role="assistant", content="Reply 2")

    outcome = sink.rewind_user_turn("sess_turn", -1)
    assert outcome.rewound_count == 2
    assert outcome.turns_undone == 1

    events = tmp_event_store.session_events("sess_turn")
    undo_events = [e for e in events if e["type"] == "conversation.undone"]
    assert len(undo_events) == 1
    assert undo_events[0]["payload"]["target_ordinal"] == -1
    sink.close()


def test_replay_idempotency_without_active_filter(
    tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """Replaying events twice into a database with active and inactive messages produces NO duplicate rows."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_idemp", source="cli")
    id1 = sink.append_message("sess_idemp", role="user", content="U1")
    sink.append_message("sess_idemp", role="assistant", content="A1")
    id2 = sink.append_message("sess_idemp", role="user", content="U2")
    sink.append_message("sess_idemp", role="assistant", content="A2")
    sink.rewind_to_message("sess_idemp", target_message_id=id2)

    count_before = tmp_session_db._conn.execute("SELECT COUNT(*) FROM messages WHERE session_id = 'sess_idemp'").fetchone()[0]
    assert count_before == 4

    # Replay all events into the SAME session_db where rows already exist
    sink.replay_session("sess_idemp", session_db=tmp_session_db)

    count_after = tmp_session_db._conn.execute("SELECT COUNT(*) FROM messages WHERE session_id = 'sess_idemp'").fetchone()[0]
    active_counts = [r[0] for r in tmp_session_db._conn.execute(
        "SELECT active FROM messages WHERE session_id = 'sess_idemp' ORDER BY id"
    ).fetchall()]

    assert count_after == 4  # NO duplicate rows inserted!
    assert active_counts == [1, 1, 0, 0]  # Inactive rows remain inactive!
    sink.close()


def test_message_appended_payload_excludes_guards(
    tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """Guard parameters (compression_lock_holder, turn_lease_*, ttl) are excluded from event payload."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_guards", source="cli")
    tmp_session_db.try_acquire_session_turn_lease("sess_guards", "lease_holder_xyz", ttl_seconds=120.0)

    sink.append_message(
        "sess_guards",
        role="user",
        content="Test Guards",
        compression_lock_holder="lock_holder_xyz",
        turn_lease_holder="lease_holder_xyz",
        turn_lease_ttl_seconds=120.0,
    )

    events = tmp_event_store.session_events("sess_guards")
    msg_event = next(e for e in events if e["type"] == "message.appended")
    payload = msg_event["payload"]

    # Excluded
    assert "compression_lock_holder" not in payload
    assert "turn_lease_holder" not in payload
    assert "turn_lease_ttl_seconds" not in payload

    # Included
    assert payload["role"] == "user"
    assert payload["content"] == "Test Guards"
    assert "message_uid" in payload
    sink.close()


def test_failure_injection_state_db_fails_is_fatal_raises_to_caller(
    tmp_session_db: SessionDB, tmp_event_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fatal failure verification: state.db failure raises exception to caller, but event is preserved in events.db and catch-up is possible."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_fatal", source="cli")

    # Inject failure into session_db.append_message
    original_append = tmp_session_db.append_message

    def failing_append(*args: Any, **kwargs: Any) -> Any:
        raise sqlite3.OperationalError("database is locked (injected failure)")

    monkeypatch.setattr(tmp_session_db, "append_message", failing_append)

    # Secondary write failure MUST be fatal: exception propagates directly to caller
    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        sink.append_message("sess_fatal", role="user", content="Fatal Message")

    # Event WAS committed to events.db before state.db failed
    events = tmp_event_store.session_events("sess_fatal")
    assert any(e["type"] == "message.appended" for e in events)

    # State.db did NOT receive the message during the failed call
    c = tmp_session_db._conn.execute("SELECT COUNT(*) FROM messages WHERE session_id = 'sess_fatal'").fetchone()[0]
    assert c == 0

    # Restore session_db and catch up via replay_pending()
    monkeypatch.setattr(tmp_session_db, "append_message", original_append)
    replayed = sink.replay_pending("sess_fatal")
    assert replayed >= 1

    # Message now exists in state.db
    row = tmp_session_db._conn.execute("SELECT content FROM messages WHERE session_id = 'sess_fatal'").fetchone()
    assert row["content"] == "Fatal Message"
    sink.close()


def test_tool_call_uids_and_tool_call_uid_preserved_in_payload_and_replay(
    tmp_session_db: SessionDB, tmp_event_store: EventStore, tmp_path: Path
) -> None:
    """Verify tool_call_uids and tool_call_uid are carried in message.appended payload and preserved during projection without re-minting."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    s_id = "sess_uids"
    sink.create_session(s_id, source="cli")

    # Assistant turn with tool calls
    tcs = [{"id": "call_1", "type": "function", "function": {"name": "test_tool", "arguments": "{}"}}]
    custom_uids = {"call_1": "uid_custom_456"}
    asst_id = sink.append_message(
        s_id,
        role="assistant",
        content="calling tool",
        tool_calls=tcs,
        tool_call_uids=custom_uids,
    )

    # Tool result
    tool_id = sink.append_message(
        s_id,
        role="tool",
        content="result",
        tool_call_id="call_1",
        tool_call_uid="uid_custom_456",
    )

    # Check state.db values
    row_asst = tmp_session_db._conn.execute("SELECT tool_call_uids FROM messages WHERE id = ?", (asst_id,)).fetchone()
    row_tool = tmp_session_db._conn.execute("SELECT tool_call_uid FROM messages WHERE id = ?", (tool_id,)).fetchone()
    assert json.loads(row_asst["tool_call_uids"]) == custom_uids
    assert row_tool["tool_call_uid"] == "uid_custom_456"

    # Check events.db payload
    events = tmp_event_store.session_events(s_id)
    msg_events = [e for e in events if e["type"] == "message.appended"]
    assert len(msg_events) == 2
    assert msg_events[0]["payload"]["tool_call_uids"] == custom_uids
    assert msg_events[1]["payload"]["tool_call_uid"] == "uid_custom_456"

    # Replay into fresh SessionDB — MUST NOT re-mint UIDs
    fresh_db = SessionDB(db_path=tmp_path / "fresh_replay.db")
    sink.replay_session(s_id, session_db=fresh_db)

    replayed_asst = fresh_db._conn.execute("SELECT tool_call_uids FROM messages WHERE session_id = ? AND role = 'assistant'", (s_id,)).fetchone()
    replayed_tool = fresh_db._conn.execute("SELECT tool_call_uid FROM messages WHERE session_id = ? AND role = 'tool'", (s_id,)).fetchone()

    assert json.loads(replayed_asst["tool_call_uids"]) == custom_uids
    assert replayed_tool["tool_call_uid"] == "uid_custom_456"

    sink.close()
    fresh_db.close()


def test_failure_injection_event_store_fails_aborts_state_db(
    tmp_session_db: SessionDB, tmp_event_store: EventStore
) -> None:
    """Fail-fast verification: if event_store.emit fails, state.db is never touched."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_abort", source="cli")

    # Pass an invalid actor format (no colon) which violates D-05
    with pytest.raises(ValueError, match="Invalid actor format"):
        sink.append_message("sess_abort", role="user", content="Will Abort", actor="invalid_actor")

    # Verify state.db was never touched
    c = tmp_session_db._conn.execute("SELECT COUNT(*) FROM messages WHERE session_id = 'sess_abort'").fetchone()[0]
    assert c == 0
    sink.close()
