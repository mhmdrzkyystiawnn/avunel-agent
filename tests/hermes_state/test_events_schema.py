"""F0-02: the ``events`` table — schema, append-only guard, D-04/D-06/D-07/D-08 constraints.

Every test builds its own ``events.db`` inside pytest's ``tmp_path``. The sandbox fixture at
``~/avunel-events-data/state.db`` is never opened here (see planning §3.6 for the G0a-1 policy).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hermes_state_events import (
    EVENTS_SCHEMA_VERSION,
    PAYLOAD_JSON_MAX_BYTES,
    EventStore,
    events_db_path,
)

K7_COLUMNS = {
    "id",
    "parent_id",
    "ts",
    "type",
    "v",
    "session_id",
    "branch_id",
    "actor",
    "correlation_id",
    "payload_json",
    "api_content_ref",
    "snapshot_ref",
    "created_at",
}


@pytest.fixture()
def store(tmp_path: Path) -> EventStore:
    return EventStore(tmp_path / "events.db")


def _columns(store: EventStore) -> set[str]:
    rows = store._conn().execute("PRAGMA table_info(events)").fetchall()
    return {row["name"] for row in rows}


# --- schema ------------------------------------------------------------------


def test_schema_is_exactly_k7_13_columns(store: EventStore) -> None:
    assert _columns(store) == K7_COLUMNS, (
        f"events must have exactly the 13 K7 columns; got {sorted(_columns(store))}"
    )


def test_branches_table_exists_with_fk(store: EventStore) -> None:
    with store._write() as conn:
        conn.execute("INSERT INTO branches (id, label) VALUES ('b1', 'main')")
        conn.execute(
            "INSERT INTO events (ts, type, session_id, branch_id, payload_json)"
            " VALUES (1, 'state.x', 's1', 'b1', '{}')"
        )
    row = store._conn().execute(
        "SELECT branch_id FROM events WHERE session_id='s1'"
    ).fetchone()
    assert row["branch_id"] == "b1"


def test_branch_fk_rejects_unknown_id(store: EventStore) -> None:
    with pytest.raises(Exception, match="FOREIGN KEY"):
        with store._write() as conn:
            conn.execute(
                "INSERT INTO events (ts, type, session_id, branch_id, payload_json)"
                " VALUES (1, 'state.x', 's1', 'no-such-branch', '{}')"
            )


def test_schema_version_stamped(store: EventStore) -> None:
    row = store._conn().execute(
        "SELECT value FROM meta WHERE key='schema_version'"
    ).fetchone()
    assert row["value"] == str(EVENTS_SCHEMA_VERSION)


# --- D-06: payload_json byte cap --------------------------------------------


def test_payload_65536_bytes_accepted(store: EventStore) -> None:
    # JSON encoding adds 12 bytes overhead ({"blob":"","}) → 65524 chars of x
    store.emit("state.x", ts=1, payload={"blob": "x" * 65524})
    row = store._conn().execute("SELECT payload_json FROM events WHERE id=1").fetchone()
    assert len(row["payload_json"].encode("utf-8")) <= PAYLOAD_JSON_MAX_BYTES


def test_payload_65537_bytes_rejected(store: EventStore) -> None:
    with pytest.raises(Exception, match="CHECK"):
        store.emit("state.x", ts=1, payload={"blob": "x" * 65525})


def test_payload_byte_not_character_count(store: EventStore) -> None:
    # é = 2 bytes in UTF-8. 32762 é → 65536 bytes total: passes (byte semantics, not char count).
    store.emit("state.x", ts=1, payload={"blob": "é" * 32762})
    row = store._conn().execute("SELECT length(CAST(payload_json AS BLOB)) FROM events").fetchone()
    assert row[0] == PAYLOAD_JSON_MAX_BYTES


def test_payload_byte_semantics_rejects_utf8(store: EventStore) -> None:
    # 32763 é → 65538 bytes: rejected despite being fewer characters than the ASCII case above.
    with pytest.raises(Exception, match="CHECK"):
        store.emit("state.x", ts=1, payload={"blob": "é" * 32763})


# --- D-07: api_content_ref required only for message.* / generation.* -------


def test_message_event_without_api_content_ref_rejected(store: EventStore) -> None:
    with pytest.raises(Exception, match="CHECK"):
        store.emit("message.sent", ts=1, payload={"message_uid": "uid-test-1"})


def test_message_event_with_api_content_ref_accepted(store: EventStore) -> None:
    eid = store.emit("message.sent", ts=1, payload={"message_uid": "uid-test-2"}, api_content_ref="ref-1")
    assert eid == 1


def test_generation_event_without_api_content_ref_rejected(store: EventStore) -> None:
    with pytest.raises(Exception, match="CHECK"):
        store.emit("generation.start", ts=1, payload={})


def test_generation_event_with_api_content_ref_accepted(store: EventStore) -> None:
    eid = store.emit("generation.start", ts=1, payload={}, api_content_ref="ref-2")
    assert eid >= 1


def test_non_message_event_without_api_content_ref_accepted(store: EventStore) -> None:
    eid = store.emit("state.session", ts=1, payload={})
    assert eid >= 1


def test_session_event_with_api_content_ref_accepted(store: EventStore) -> None:
    # The CHECK forbids NULLs only for message.*/generation.*; a ref on other types is allowed.
    eid = store.emit("session.created", ts=1, payload={}, api_content_ref="ref-x")
    assert eid >= 1


# --- D-08: created_at DEFAULT ------------------------------------------------


def test_created_at_default_populated(store: EventStore) -> None:
    store.emit("state.x", ts=1, payload={})
    row = store._conn().execute("SELECT created_at FROM events WHERE id=1").fetchone()
    assert row["created_at"] is not None
    # SQLite datetime('now') -> 'YYYY-MM-DD HH:MM:SS' UTC
    assert len(row["created_at"]) == 19


def test_created_at_explicit_value_stored(store: EventStore) -> None:
    """D-08: emit() does not expose created_at (enforcement there). Raw SQL explicit insert is stored."""
    with store._write() as conn:
        conn.execute(
            "INSERT INTO events (ts, type, payload_json, created_at)"
            " VALUES (1, 'state.x', '{}', '2020-01-01 00:00:00')"
        )
    row = store._conn().execute("SELECT created_at FROM events WHERE id=1").fetchone()
    assert row["created_at"] == "2020-01-01 00:00:00"


# --- append-only guard ------------------------------------------------------


def test_update_rejected(store: EventStore) -> None:
    store.emit("state.x", ts=1, payload={})
    with pytest.raises(Exception, match="append-only"):
        store._conn().execute("UPDATE events SET payload_json='{}' WHERE id=1")


def test_delete_rejected(store: EventStore) -> None:
    store.emit("state.x", ts=1, payload={})
    with pytest.raises(Exception, match="append-only"):
        store._conn().execute("DELETE FROM events WHERE id=1")


# --- basic read paths --------------------------------------------------------


def test_emit_and_get(store: EventStore) -> None:
    eid = store.emit("state.x", ts=1, payload={"a": 1}, actor="cli:s1")
    row = store.get_event(eid)
    assert row is not None
    assert row["id"] == eid
    assert row["type"] == "state.x"
    assert row["ts"] == 1
    assert json.loads(row["payload_json"]) == {"a": 1}
    assert row["actor"] == "cli:s1"


def test_events_after_in_id_order(store: EventStore) -> None:
    for i in range(5):
        store.emit("state.x", ts=i, payload={})
    rows = list(store.events_after(2))
    assert [r["id"] for r in rows] == [3, 4, 5]


def test_all_events_yields_in_order(store: EventStore) -> None:
    for i in range(3):
        store.emit("state.x", ts=i, payload={})
    rows = list(store.all_events())
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_count(store: EventStore) -> None:
    assert store.count() == 0
    store.emit("state.x", ts=1, payload={})
    store.emit("state.x", ts=2, payload={})
    assert store.count() == 2


# --- default path -----------------------------------------------------------


def test_events_db_path_is_under_hermes_home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    # Re-import to pick up the env change
    import importlib
    import hermes_state_events

    importlib.reload(hermes_state_events)
    assert hermes_state_events.events_db_path() == tmp_path / "events.db"
