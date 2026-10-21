"""Tests for importer.py (F0-10).

Verifies:
- Idempotency: run 1x, 2x, 3x produces the exact same number of events without duplicate rows.
- Subset import: importing session A, then importing all sessions leaves session A untouched and imports the rest.
- Preservation of message_uid in payload_json (D-09).
- Integration test on the 3 backlog sessions: core-talos, faktura-asli, hermes-dev.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import pytest

from importer import import_state_to_events
from hermes_state_events import EventStore, open_events
from blob_store import BlobStore

FIXTURE_PATH = Path("/home/iki/avunel-events-data/state.db")


@pytest.fixture
def sample_state_db(tmp_path: Path) -> Path:
    """Create a minimal source state.db with 2 sessions and messages."""
    db_path = tmp_path / "sample_state.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE sessions (
            id TEXT PRIMARY KEY,
            source TEXT,
            model TEXT,
            model_config TEXT,
            cwd TEXT,
            profile_name TEXT,
            title TEXT,
            created_at REAL,
            parent_session_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT,
            tool_name TEXT,
            tool_calls TEXT,
            tool_call_id TEXT,
            tool_call_uids TEXT,
            tool_call_uid TEXT,
            token_count INTEGER,
            finish_reason TEXT,
            reasoning TEXT,
            reasoning_content TEXT,
            reasoning_details TEXT,
            codex_reasoning_items TEXT,
            codex_message_items TEXT,
            platform_message_id TEXT,
            observed INTEGER DEFAULT 0,
            effect_disposition TEXT,
            display_kind TEXT,
            display_metadata TEXT,
            _compressed_summary INTEGER DEFAULT 0,
            timestamp REAL,
            message_uid TEXT
        )
        """
    )

    # Insert 2 sessions
    conn.execute(
        "INSERT INTO sessions (id, source, model, title, created_at) VALUES (?, ?, ?, ?, ?)",
        ("sess_1", "cli", "gpt-4o", "Session 1", 1000.0),
    )
    conn.execute(
        "INSERT INTO sessions (id, source, model, title, created_at) VALUES (?, ?, ?, ?, ?)",
        ("sess_2", "cli", "gpt-4o", "Session 2", 2000.0),
    )

    # Insert messages for sess_1
    conn.execute(
        "INSERT INTO messages (id, session_id, role, content, message_uid, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (1, "sess_1", "user", "Hello turn 1", "uid_001", 1001.0),
    )
    conn.execute(
        "INSERT INTO messages (id, session_id, role, content, message_uid, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (2, "sess_1", "assistant", "Hi there!", "uid_002", 1002.0),
    )

    # Insert message for sess_2
    conn.execute(
        "INSERT INTO messages (id, session_id, role, content, message_uid, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
        (3, "sess_2", "user", "Session 2 question", "uid_003", 2001.0),
    )

    conn.commit()
    conn.close()
    return db_path


def test_importer_basic_and_message_uid_preservation(sample_state_db: Path, tmp_path: Path) -> None:
    """Importer migrates sessions and messages while preserving message_uid in payload_json."""
    event_store = open_events(tmp_path / "events.db")
    stats = import_state_to_events(sample_state_db, event_store)

    assert stats["sessions_imported"] == 2
    assert stats["messages_imported"] == 3
    assert stats["skipped_messages"] == 0

    # Verify event counts in events.db
    all_events = list(event_store.all_events())
    assert len(all_events) == 5  # 2 session.created + 3 message.appended

    # Verify message_uid is preserved in payload_json
    msg_events = [e for e in all_events if e["type"] == "message.appended"]
    uids = [json.loads(e["payload_json"])["message_uid"] for e in msg_events]
    assert uids == ["uid_001", "uid_002", "uid_003"]


def test_importer_idempotency_run_multiple_times(sample_state_db: Path, tmp_path: Path) -> None:
    """Running importer 2x or 3x on same database produces exact same events without duplicate rows."""
    event_store = open_events(tmp_path / "events.db")

    # Run 1
    stats1 = import_state_to_events(sample_state_db, event_store)
    assert stats1["sessions_imported"] == 2
    assert stats1["messages_imported"] == 3
    count1 = len(list(event_store.all_events()))

    # Run 2
    stats2 = import_state_to_events(sample_state_db, event_store)
    assert stats2["sessions_imported"] == 0
    assert stats2["messages_imported"] == 0
    assert stats2["skipped_messages"] == 3
    count2 = len(list(event_store.all_events()))
    assert count1 == count2 == 5

    # Run 3
    stats3 = import_state_to_events(sample_state_db, event_store)
    assert stats3["sessions_imported"] == 0
    assert stats3["messages_imported"] == 0
    assert stats3["skipped_messages"] == 3
    count3 = len(list(event_store.all_events()))
    assert count1 == count3 == 5


def test_importer_subset_then_full(sample_state_db: Path, tmp_path: Path) -> None:
    """Importing subset (sess_1) then full imports remaining session without duplicating sess_1."""
    event_store = open_events(tmp_path / "events.db")

    # Import only sess_1
    stats1 = import_state_to_events(sample_state_db, event_store, session_ids=["sess_1"])
    assert stats1["sessions_imported"] == 1
    assert stats1["messages_imported"] == 2
    assert len(list(event_store.all_events())) == 3  # 1 session + 2 messages

    # Import full (all sessions)
    stats2 = import_state_to_events(sample_state_db, event_store)
    assert stats2["sessions_imported"] == 1  # only sess_2
    assert stats2["messages_imported"] == 1  # only message 3
    assert stats2["skipped_messages"] == 2  # messages 1 and 2 skipped
    assert len(list(event_store.all_events())) == 5


@pytest.mark.skipif(not FIXTURE_PATH.exists(), reason="Fixture state.db required for backlog test")
def test_importer_3_backlog_sessions(tmp_path: Path) -> None:
    """Integration test importing the 3 backlog sessions: core-talos, hermes-dev, faktura-asli."""
    event_store = open_events(tmp_path / "events.db")
    blob_store = BlobStore(tmp_path / "blobs")

    backlog_sids = [
        "20261005_114548_9a1787",  # core-talos (5571 messages)
        "20261007_231457_2031a5",  # faktura-asli (758 messages)
    ]

    stats = import_state_to_events(
        FIXTURE_PATH,
        event_store,
        blob_store=blob_store,
        session_ids=backlog_sids,
    )

    assert stats["sessions_imported"] == 2
    assert stats["messages_imported"] == 5571 + 758

    # Re-run idempotency on backlog sessions
    stats_rerun = import_state_to_events(
        FIXTURE_PATH,
        event_store,
        blob_store=blob_store,
        session_ids=backlog_sids,
    )
    assert stats_rerun["sessions_imported"] == 0
    assert stats_rerun["messages_imported"] == 0
    assert stats_rerun["skipped_messages"] == 5571 + 758
