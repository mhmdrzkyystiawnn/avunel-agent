"""F0-04: tests for emit() API, actor validation (D-05), message_uid validation (D-09),
deterministic ordering (ORDER BY id ASC), and payload upgrader chaining.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hermes_state_events import EventStore


@pytest.fixture()
def store(tmp_path: Path) -> EventStore:
    return EventStore(tmp_path / "events.db")


# --- Sequential IDs & Basic Emission ----------------------------------------


def test_emit_returns_sequential_ids(store: EventStore) -> None:
    e1 = store.emit("state.a", ts=10, payload={"x": 1})
    e2 = store.emit("state.b", ts=20, payload={"x": 2})
    e3 = store.emit("state.c", ts=30, payload={"x": 3})
    assert e1 == 1
    assert e2 == 2
    assert e3 == 3


def test_emit_validates_v_and_ts(store: EventStore) -> None:
    with pytest.raises(ValueError, match="v must be a positive integer"):
        store.emit("state.x", ts=1, v=0)

    with pytest.raises(ValueError, match="v must be a positive integer"):
        store.emit("state.x", ts=1, v=-5)

    with pytest.raises(TypeError, match="ts must be an int"):
        store.emit("state.x", ts="not-an-int")  # type: ignore[arg-type]


# --- Actor Validation (D-05) ------------------------------------------------


def test_emit_validates_actor_format(store: EventStore) -> None:
    # Valid actors: 'domain:id' with non-empty parts
    assert store.emit("state.x", ts=1, actor="cli:s1") == 1
    assert store.emit("state.x", ts=2, actor="tui:session-123") == 2
    assert store.emit("state.x", ts=3, actor="desktop:client-abc") == 3
    assert store.emit("state.x", ts=4, actor="cron:daily-backup") == 4
    assert store.emit("state.x", ts=5, actor="agent:subagent-42") == 5

    # None is allowed (optional field)
    assert store.emit("state.x", ts=6, actor=None) == 6

    # Invalid actor formats
    with pytest.raises(ValueError, match="Invalid actor format"):
        store.emit("state.x", ts=7, actor="cli")  # missing colon

    with pytest.raises(ValueError, match="Invalid actor format"):
        store.emit("state.x", ts=8, actor="cli:")  # empty id

    with pytest.raises(ValueError, match="Invalid actor format"):
        store.emit("state.x", ts=9, actor=":s1")  # empty domain

    with pytest.raises(ValueError, match="Invalid actor format"):
        store.emit("state.x", ts=10, actor="   :   ")  # whitespace only

    with pytest.raises(TypeError, match="actor must be a str"):
        store.emit("state.x", ts=11, actor=123)  # type: ignore[arg-type]


# --- Payload Structure & message_uid Enforcement (D-09) ---------------------


def test_emit_rejects_non_dict_payload(store: EventStore) -> None:
    with pytest.raises(TypeError, match="payload must be a dict"):
        store.emit("state.x", ts=1, payload=["a", "b"])  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="payload must be a dict"):
        store.emit("state.x", ts=2, payload="raw string")  # type: ignore[arg-type]

    # None and empty dict are valid
    assert store.emit("state.x", ts=3, payload=None) == 1
    assert store.emit("state.x", ts=4, payload={}) == 2


def test_emit_message_event_requires_message_uid(store: EventStore) -> None:
    # message.* without message_uid in payload must be rejected per D-09
    with pytest.raises(ValueError, match="requires a non-empty string 'message_uid'"):
        store.emit("message.user", ts=1, payload={}, api_content_ref="ref-1")

    with pytest.raises(ValueError, match="requires a non-empty string 'message_uid'"):
        store.emit("message.assistant", ts=2, payload={"content": "hello"}, api_content_ref="ref-2")

    with pytest.raises(ValueError, match="requires a non-empty string 'message_uid'"):
        store.emit("message.user", ts=3, payload={"message_uid": ""}, api_content_ref="ref-3")

    with pytest.raises(ValueError, match="requires a non-empty string 'message_uid'"):
        store.emit("message.user", ts=4, payload={"message_uid": "   "}, api_content_ref="ref-4")

    with pytest.raises(ValueError, match="requires a non-empty string 'message_uid'"):
        store.emit("message.user", ts=5, payload={"message_uid": 12345}, api_content_ref="ref-5")  # type: ignore[dict-item]

    # Valid message_uid
    eid = store.emit(
        "message.user",
        ts=6,
        payload={"message_uid": "uid-001", "content": "hello"},
        api_content_ref="ref-6",
    )
    assert eid > 0
    event = store.get_event(eid)
    assert event is not None
    assert event["payload"]["message_uid"] == "uid-001"


def test_emit_non_message_events_do_not_require_message_uid(store: EventStore) -> None:
    # Other event types do not need message_uid
    e1 = store.emit("session.created", ts=1, payload={"title": "new session"})
    e2 = store.emit("generation.start", ts=2, payload={"model": "claude"}, api_content_ref="ref-gen")
    e3 = store.emit("state.checkpoint", ts=3, payload={"step": 5})
    assert e1 > 0
    assert e2 > 0
    assert e3 > 0


# --- Ordering: Global Order is Commit Order (id ASC), Not ts ----------------


def test_readers_order_by_id_asc_not_ts(store: EventStore) -> None:
    # Insert events with timestamps strictly out of order
    e1 = store.emit("state.x", ts=1000, session_id="s1", payload={"marker": 1})
    e2 = store.emit("state.x", ts=500, session_id="s1", payload={"marker": 2})   # earlier ts, later commit
    e3 = store.emit("state.x", ts=2000, session_id="s1", payload={"marker": 3})
    e4 = store.emit("state.x", ts=100, session_id="s1", payload={"marker": 4})   # earliest ts, latest commit

    assert [e1, e2, e3, e4] == [1, 2, 3, 4]

    # 1. all_events() must follow id ASC [1, 2, 3, 4], not ts [4, 2, 1, 3]
    all_rows = list(store.all_events())
    assert [r["id"] for r in all_rows] == [1, 2, 3, 4]
    assert [r["ts"] for r in all_rows] == [1000, 500, 2000, 100]

    # 2. events_after(last_id) must follow id ASC
    after_2 = store.events_after(2)
    assert [r["id"] for r in after_2] == [3, 4]

    # 3. session_events(session_id) must follow id ASC
    sess_rows = store.session_events("s1")
    assert [r["id"] for r in sess_rows] == [1, 2, 3, 4]


# --- Payload Upgrader Chaining & Interaction with Redactions (Point 5) ------


def test_register_payload_upgrader_validation(store: EventStore) -> None:
    with pytest.raises(ValueError, match="to_v .* must be greater than from_v"):
        store.register_payload_upgrader("state.x", from_v=1, to_v=1, fn=lambda p: p)

    with pytest.raises(ValueError, match="to_v .* must be greater than from_v"):
        store.register_payload_upgrader("state.x", from_v=2, to_v=1, fn=lambda p: p)


def test_payload_upgrader_chaining(store: EventStore) -> None:
    # Register chained upgraders for 'config.item': v1 -> v2 -> v3
    def v1_to_v2(payload: dict) -> dict:
        p = dict(payload)
        p["key"] = p.pop("k")
        return p

    def v2_to_v3(payload: dict) -> dict:
        p = dict(payload)
        p["migrated_to_v3"] = True
        return p

    store.register_payload_upgrader("config.item", from_v=1, to_v=2, fn=v1_to_v2)
    store.register_payload_upgrader("config.item", from_v=2, to_v=3, fn=v2_to_v3)

    # Emit event at v1
    eid = store.emit("config.item", ts=1, v=1, payload={"k": "debug_mode", "val": 1})

    event = store.get_event(eid)
    assert event is not None
    # v is upgraded to 3
    assert event["v"] == 3
    # payload is fully upgraded through the chain
    assert event["payload"] == {"key": "debug_mode", "val": 1, "migrated_to_v3": True}
    # payload_json string matches upgraded payload
    assert json.loads(event["payload_json"]) == {"key": "debug_mode", "val": 1, "migrated_to_v3": True}


def test_payload_upgrader_with_redacted_event(store: EventStore) -> None:
    """Ensure payload upgrader runs cleanly on an event that was previously redacted."""
    raw_secret = "".join(["g", "h", "p", "_", "123456789012345678901234567890123456"])

    # Emit v1 event unredacted
    eid = store.emit(
        "credential.item",
        ts=1,
        v=1,
        payload={"legacy_token": raw_secret, "label": "github"},
        redact=False,
    )

    # Redact event via redactions overlay
    store.redact(
        eid,
        reason="audit leak",
        replacement_payload={"legacy_token": raw_secret, "label": "github_redacted"},
    )

    # Register upgrader v1 -> v2
    def v1_to_v2(payload: dict) -> dict:
        p = dict(payload)
        p["token"] = p.pop("legacy_token")
        p["schema_version"] = 2
        return p

    store.register_payload_upgrader("credential.item", from_v=1, to_v=2, fn=v1_to_v2)

    # Read event: should have BOTH the redaction (from redactions table) AND the upgrade (v1 -> v2)
    event = store.get_event(eid)
    assert event is not None
    assert event["v"] == 2
    assert "legacy_token" not in event["payload"]
    assert "token" in event["payload"]
    assert "".join(["g", "h", "p", "_", "12"]) in event["payload"]["token"]
    assert raw_secret not in event["payload"]["token"]
    assert event["payload"]["label"] == "github_redacted"
    assert event["payload"]["schema_version"] == 2
