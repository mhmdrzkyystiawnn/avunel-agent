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
        content="My token is " + "".join(["s", "k", "-", "1234567890abcdef1234567890abcdef"]) + "",
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
    assert "" + "".join(["s", "k", "-", "1234567890abcdef1234567890abcdef"]) + "" in m1_rows[0]["content"]
    assert "" + "".join(["s", "k", "-", "1234567890abcdef1234567890abcdef"]) + "" not in m2_rows[0]["content"]
    assert "" + "".join(["s", "k", "-", "123...cdef"]) + "" in m2_rows[0]["content"]

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


def test_session_with_reused_message_uid_projects_both_rows(
    tmp_path: Path, tmp_event_store: EventStore
) -> None:
    """Sessions with reused message_uid project BOTH rows into state.db (contract: event_id set, not uid)."""
    fresh_db = SessionDB(db_path=tmp_path / "fresh_uid.db")
    sink = DualSink(session_db=fresh_db, event_store=tmp_event_store, mode="dual")
    sid = "sess_reused_uid"
    sink.create_session(sid, source="cli")

    shared_uid = "shared_uid_12345"
    # Event 1: older message with shared_uid
    tmp_event_store.emit(
        type="message.appended",
        ts=1000,
        session_id=sid,
        payload={
            "session_id": sid,
            "role": "user",
            "content": "First occurrence",
            "message_uid": shared_uid,
        },
        api_content_ref=f"msg:{shared_uid}",
    )
    # Event 2: newer message with the same shared_uid
    tmp_event_store.emit(
        type="message.appended",
        ts=2000,
        session_id=sid,
        payload={
            "session_id": sid,
            "role": "user",
            "content": "Second occurrence (active)",
            "message_uid": shared_uid,
        },
        api_content_ref=f"msg:{shared_uid}",
    )

    # Replay all events into state.db
    projected = sink.replay_session(sid, session_db=fresh_db)
    assert projected == 2  # 2 message.appended (session.created was already projected during create_session)

    # Verify BOTH rows exist in state.db with shared_uid
    rows = fresh_db._conn.execute(
        "SELECT id, content, message_uid FROM messages WHERE session_id = ? ORDER BY id ASC",
        (sid,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["content"] == "First occurrence"
    assert rows[1]["content"] == "Second occurrence (active)"
    assert rows[0]["message_uid"] == rows[1]["message_uid"] == shared_uid
    sink.close()
    fresh_db.close()


def test_orphan_event_after_state_db_failure_caught_up_by_replay_pending(
    tmp_session_db: SessionDB, tmp_event_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Orphan event (state.db failed after emit, next event succeeded) is caught up by replay_pending."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sid = "sess_orphan"
    sink.create_session(sid, source="cli")

    # Event 1: succeeds
    m1_id = sink.append_message(sid, role="user", content="Msg 1")
    assert m1_id is not None

    # Event 2: state.db failure injected -> orphan event in events.db
    original_append = tmp_session_db.append_message

    def failing_append(*args: Any, **kwargs: Any) -> Any:
        raise sqlite3.OperationalError("database is locked (orphan injection)")

    monkeypatch.setattr(tmp_session_db, "append_message", failing_append)

    with pytest.raises(sqlite3.OperationalError):
        sink.append_message(sid, role="assistant", content="Msg 2 (Orphan)")

    # Restore append_message
    monkeypatch.setattr(tmp_session_db, "append_message", original_append)

    # Event 3: succeeds
    m3_id = sink.append_message(sid, role="user", content="Msg 3")
    assert m3_id is not None

    # Current state in state.db: only Msg 1 and Msg 3 exist
    contents_before = [
        r["content"]
        for r in tmp_session_db._conn.execute(
            "SELECT content FROM messages WHERE session_id = ? ORDER BY id ASC", (sid,)
        ).fetchall()
    ]
    assert contents_before == ["Msg 1", "Msg 3"]

    # Catch up orphan events via replay_pending()
    replayed = sink.replay_pending(sid)
    assert replayed == 1  # Exactly the orphan event (Msg 2) was replayed

    # Check final messages in state.db
    rows_after = tmp_session_db._conn.execute(
        "SELECT id, content FROM messages WHERE session_id = ? ORDER BY id ASC", (sid,)
    ).fetchall()
    assert len(rows_after) == 3
    # Report row order in state.db: Msg 1 (id=1), Msg 3 (id=2), Msg 2 (id=3)
    assert [r["content"] for r in rows_after] == ["Msg 1", "Msg 3", "Msg 2 (Orphan)"]
    sink.close()


def test_failure_inside_transaction_rollbacks_row_and_event_id(
    tmp_session_db: SessionDB, tmp_event_store: EventStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure inside transaction after insert rolls back both messages row AND events_projected."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sid = "sess_tx_fail"
    sink.create_session(sid, source="cli")

    # Emit an event in events.db
    ev = tmp_event_store.emit(
        type="message.appended",
        ts=1000,
        session_id=sid,
        payload={
            "session_id": sid,
            "role": "user",
            "content": "Will Rollback",
            "message_uid": "uid_rollback",
        },
        api_content_ref="msg:uid_rollback",
    )

    # Monkeypatch _bump_session_counters to raise an error inside _do(conn)
    def failing_bump(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("crash after insert inside transaction")

    monkeypatch.setattr(tmp_session_db, "_bump_session_counters", failing_bump)

    with pytest.raises(RuntimeError, match="crash after insert inside transaction"):
        sink.project_event(ev, tmp_session_db)

    # Check state.db: message row was rolled back
    m_count = tmp_session_db._conn.execute(
        "SELECT COUNT(*) FROM messages WHERE session_id = ?", (sid,)
    ).fetchone()[0]
    assert m_count == 0

    # Check events_projected: event_id was ALSO rolled back!
    proj = tmp_session_db._conn.execute(
        "SELECT 1 FROM events_projected WHERE event_id = ?", (ev if isinstance(ev, int) else ev["id"],)
    ).fetchone()
    assert proj is None
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


def test_crash_during_replay_pending_resumes_cleanly(tmp_path: Path, tmp_session_db: SessionDB, tmp_event_store: EventStore) -> None:
    """Replay pending handles mid-replay crash cleanly and produces identical final state when resumed."""
    sink = DualSink(session_db=tmp_session_db, event_store=tmp_event_store, mode="dual")
    sink.create_session("sess_crash", source="cli")

    # Append 6 messages directly to event store
    for i in range(1, 7):
        tmp_event_store.emit(
            "message.appended",
            ts=i,
            session_id="sess_crash",
            payload={"message_uid": f"msg_{i}", "role": "user", "content": f"text {i}"},
            api_content_ref=f"msg:msg_{i}",
        )

    # Simulate crash by throwing on the 4th projected event
    original_project = sink.project_event
    call_count = 0
    def crashing_project(*args: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 4:
            raise RuntimeError("simulated crash at event 4")
        return original_project(*args, **kwargs)

    sink.project_event = crashing_project
    with pytest.raises(RuntimeError, match="simulated crash at event 4"):
        sink.replay_pending()

    # Verify state after crash: 1 session event + 3 messages projected
    c1 = tmp_session_db._conn.execute("SELECT count(*) FROM messages WHERE session_id = ?", ("sess_crash",)).fetchone()[0]
    p1 = tmp_session_db._conn.execute("SELECT count(*) FROM events_projected").fetchone()[0]
    assert c1 == 3
    assert p1 == 4

    # Restore normal project and resume replay_pending()
    sink.project_event = original_project
    projected_resumed = sink.replay_pending()
    assert projected_resumed == 3  # Remaining 3 message events projected

    # Final count must be exactly 6 messages and 7 events_projected (1 session + 6 messages)
    c2 = tmp_session_db._conn.execute("SELECT count(*) FROM messages WHERE session_id = ?", ("sess_crash",)).fetchone()[0]
    p2 = tmp_session_db._conn.execute("SELECT count(*) FROM events_projected").fetchone()[0]
    assert c2 == 6
    assert p2 == 7
