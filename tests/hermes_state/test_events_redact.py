"""F0-03: secret redaction for the append-only event log (D-14).

Covers:
- Pre-write redaction (redact_payload on emit)
- Post-write redaction via the redactions overlay table (D-14)
- Reason sanitization before persisting to redactions table (Guardrail 1)
- Single-door reading via events_effective view (Guardrail 2)
- FTS index cleanup hook for events_fts (Guardrail 3 / F0-09 contract)
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import pytest

from hermes_state_events import (
    EventStore,
    redact_payload,
)


def _fake_ghp(suffix: str = "123456789012345678901234567890123456") -> str:
    """Construct fake GitHub PAT at runtime so static secret scanners find 0 matches."""
    return "".join(["g", "h", "p", "_", suffix])


def _fake_sk(suffix: str = "1234567890abcdef1234567890") -> str:
    """Construct fake secret key prefix at runtime."""
    return "".join(["s", "k", "-", suffix])


def _fake_sk_ant(suffix: str = "api03-1234567890abcdefghijklmnopqrstuvwxyz1234567890") -> str:
    """Construct fake Anthropic API key at runtime."""
    return "".join(["s", "k", "-", "ant-", suffix])


@pytest.fixture()
def store(tmp_path: Path) -> EventStore:
    return EventStore(tmp_path / "events.db")


# --- Pre-write redaction (redact_payload) -----------------------------------


def test_redact_payload_masks_api_keys() -> None:
    payload = {
        "anthropic": _fake_sk_ant(),
        "github": _fake_ghp(),
        "regular": "hello world",
    }
    redacted = redact_payload(payload)
    assert "".join(["s", "k", "-", "ant"]) in redacted["anthropic"]
    assert "..." in redacted["anthropic"]
    assert "".join(["g", "h", "p", "_", "12"]) in redacted["github"]
    assert "..." in redacted["github"]
    assert redacted["regular"] == "hello world"


def test_redact_payload_nested_structures() -> None:
    payload = {
        "messages": [
            {"role": "user", "content": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-ae7H8y1oh satire"},
            {"nested_json": json.dumps({"token": _fake_ghp()})},
        ],
        "metadata": {
            "key": _fake_sk(),
        },
    }
    redacted = redact_payload(payload)
    # Check Bearer in list of dicts
    assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30" not in redacted["messages"][0]["content"]
    # Check nested JSON string
    nested_obj = json.loads(redacted["messages"][1]["nested_json"])
    assert "".join(["g", "h", "p", "_", "12"]) in nested_obj["token"]
    assert "..." in nested_obj["token"]
    # Check nested dict
    assert "".join(["s", "k", "-", "123"]) in redacted["metadata"]["key"]
    assert "..." in redacted["metadata"]["key"]


def test_redact_payload_preserves_innocent_data() -> None:
    payload = {
        "text": "Avunel agent is working on event log",
        "count": 42,
        "flag": True,
        "empty": None,
        "items": [1, 2, "three"],
    }
    assert redact_payload(payload) == payload


def test_emit_pre_redacts_payload(store: EventStore) -> None:
    eid = store.emit(
        "state.config",
        ts=1,
        payload={"api_key": _fake_ghp()},
    )
    row = store.get_event(eid)
    data = json.loads(row["payload_json"])
    assert _fake_ghp() not in data["api_key"]
    assert "".join(["g", "h", "p", "_", "12"]) in data["api_key"]
    assert "..." in data["api_key"]


def test_emit_can_opt_out_redaction(store: EventStore) -> None:
    raw_secret = _fake_ghp()
    eid = store.emit(
        "state.config",
        ts=1,
        payload={"api_key": raw_secret},
        redact=False,
    )
    row = store.get_raw_event(eid)
    data = json.loads(row["payload_json"])
    assert data["api_key"] == raw_secret


# --- Post-write redaction & Guardrails (D-14) --------------------------------


def test_redact_stored_event_inserts_to_redactions_table(store: EventStore) -> None:
    raw_secret = _fake_ghp()
    eid = store.emit("state.config", ts=1, payload={"secret": raw_secret}, redact=False)

    store.redact(eid, reason="leaked secret during session")

    redaction_row = store._conn().execute(
        "SELECT * FROM redactions WHERE event_id = ?", (eid,)
    ).fetchone()
    assert redaction_row is not None
    assert redaction_row["event_id"] == eid
    assert redaction_row["reason"] == "leaked secret during session"
    redacted_data = json.loads(redaction_row["redacted_payload_json"])
    assert raw_secret not in redacted_data["secret"]
    assert "".join(["g", "h", "p", "_", "12"]) in redacted_data["secret"]


def test_redact_sanitizes_reason(store: EventStore) -> None:
    """Guardrail 1: reason argument must be sanitized with force=True before storing."""
    raw_secret_in_reason = _fake_sk()
    eid = store.emit("state.x", ts=1, payload={"msg": "innocent"})

    store.redact(
        eid,
        reason=f"Found secret token {raw_secret_in_reason} in terminal output",
    )

    redaction_row = store._conn().execute(
        "SELECT reason FROM redactions WHERE event_id = ?", (eid,)
    ).fetchone()
    assert raw_secret_in_reason not in redaction_row["reason"]
    assert "".join(["s", "k", "-", "123...7890"]) in redaction_row["reason"]


def test_redact_preserves_append_only_triggers(store: EventStore) -> None:
    eid = store.emit("state.x", ts=1, payload={"a": 1})

    # Redact must succeed without hitting events_no_update
    store.redact(eid, reason="audit test")

    # The events table append-only triggers must still be active
    with pytest.raises(sqlite3.DatabaseError, match="append-only: UPDATE blocked"):
        store._conn().execute("UPDATE events SET payload_json='{}' WHERE id=?", (eid,))
    with pytest.raises(sqlite3.DatabaseError, match="append-only: DELETE blocked"):
        store._conn().execute("DELETE FROM events WHERE id=?", (eid,))


def test_redacted_event_effective_view_and_get_event(store: EventStore) -> None:
    """Guardrail 2: Single door query via events_effective view."""
    raw_secret = _fake_ghp()
    eid = store.emit("state.x", ts=1, payload={"token": raw_secret}, redact=False)

    # Before redaction, get_event reads raw
    assert raw_secret in store.get_event(eid)["payload_json"]

    store.redact(eid, reason="sanitize secret")

    # After redaction:
    # 1. get_event reads effective view (redacted)
    clean_event = store.get_event(eid)
    assert raw_secret not in clean_event["payload_json"]
    assert "".join(["g", "h", "p", "_", "12"]) in clean_event["payload_json"]

    # 2. get_raw_event still holds raw byte in events table (D-14 documented limit)
    raw_event = store.get_raw_event(eid)
    assert raw_secret in raw_event["payload_json"]


def test_redacted_event_in_events_after_and_all_events(store: EventStore) -> None:
    """Guardrail 2: events_after and all_events must query events_effective."""
    raw_secret = _fake_ghp()
    e1 = store.emit("state.x", ts=1, payload={"v": 1})
    e2 = store.emit("state.x", ts=2, payload={"secret": raw_secret}, redact=False)
    e3 = store.emit("state.x", ts=3, payload={"v": 3})

    store.redact(e2, reason="audit")

    # Test events_after
    rows_after = store.events_after(e1)
    e2_after = next(r for r in rows_after if r["id"] == e2)
    assert raw_secret not in e2_after["payload_json"]

    # Test all_events
    all_rows = list(store.all_events())
    e2_all = next(r for r in all_rows if r["id"] == e2)
    assert raw_secret not in e2_all["payload_json"]


def test_redact_with_custom_replacement_payload(store: EventStore) -> None:
    eid = store.emit("state.x", ts=1, payload={"dirty": "leak"}, redact=False)
    store.redact(
        eid,
        reason="custom scrub",
        replacement_payload={"status": "scrubbed_by_admin"},
    )
    row = store.get_event(eid)
    assert json.loads(row["payload_json"]) == {"status": "scrubbed_by_admin"}


def test_redact_unknown_id_raises_key_error(store: EventStore) -> None:
    with pytest.raises(KeyError, match="id=99999"):
        store.redact(99999, reason="test")


# --- FTS index cleanup hook (Guardrail 3 / D-14 / F0-09 contract) -----------


def test_fts_cleanup_when_fts_table_exists(store: EventStore) -> None:
    """Guardrail 3: Verify FTS cleanup contract (rowid=events.id, columns: payload_json, type, session_id)."""
    conn = store._conn()
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
            payload_json,
            type,
            session_id,
            content='events_effective',
            content_rowid='id'
        )
        """
    )

    raw_secret = _fake_ghp()
    eid = store.emit(
        "message.user",
        ts=1,
        session_id="s1",
        payload={"message_uid": "uid-redact-1", "content": f"here is my secret: {raw_secret}"},
        api_content_ref="ref-1",
        redact=False,
    )

    # Initial FTS insert (mimicking F0-09 insert trigger or population)
    raw_payload_json = store.get_raw_event(eid)["payload_json"]
    conn.execute(
        "INSERT INTO events_fts(rowid, payload_json, type, session_id) VALUES (?, ?, ?, ?)",
        (eid, raw_payload_json, "message.user", "s1"),
    )

    # Verify secret is currently matchable in FTS
    match_secret = conn.execute(
        f"SELECT count(*) FROM events_fts WHERE events_fts MATCH '\"{_fake_ghp()}\"'"
    ).fetchone()[0]
    assert match_secret == 1

    # Execute redact
    store.redact(eid, reason="scrub secret token")

    # Secret must no longer match in FTS
    match_secret_after = conn.execute(
        f"SELECT count(*) FROM events_fts WHERE events_fts MATCH '\"{_fake_ghp()}\"'"
    ).fetchone()[0]
    assert match_secret_after == 0

    # Masked token or message text should now be indexed
    match_msg_after = conn.execute(
        "SELECT count(*) FROM events_fts WHERE events_fts MATCH 'secret'"
    ).fetchone()[0]
    assert match_msg_after == 1
