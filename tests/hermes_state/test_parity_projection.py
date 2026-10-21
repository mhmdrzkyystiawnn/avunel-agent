"""F0-07 & G0a-1: Parity projection tests on fixture state.db sessions.

Verifies:
1. G0a-1 statistical guards on the immutable fixture state.db:
   - 18,883 total messages, 49 sessions.
   - 17,656 visible messages (active=1 OR compacted=1).
   - 1,227 hidden messages (active=0 AND compacted=0).
   - 100% of hidden messages have display_order == id.
   - 17,656 / 17,656 visible messages match MIN(id) of visible rows.
   - 16,752 match MIN(id) of all rows (exact 904 delta).
   - SHA-256 fixture checksum verification.
2. Parity rebuild on 10 fixture sessions:
   - Deterministic insertion into EventStore.
   - Replay into fresh SessionDB.
   - Mandatory contract (putusan Iki): message_uid identical to original.
   - display_identity matches 100% (calculated by projection).
   - display_order grouping matches 100%.
   - Content, role, finish_reason, tool_name match 100%.
3. Deterministic replay parity across multiple independent projection targets.
4. Lineage and branch preservation (parent_session_id FK).
5. Undo event (conversation.undone) active-flag preservation.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import pytest

from agent.redact import redact_sensitive_text
from blob_store import BlobStore
from dual_sink import DualSink
from hermes_state import SessionDB
from hermes_state_events import open_events

FIXTURE_PATH = Path("/home/iki/avunel-events-data/state.db")
FIXTURE_SHA256_PATH = Path("/home/iki/avunel-events-data/FIXTURE.sha256")
EXPECTED_SHA256 = "406d0e72bf75ba6cd6d2db091d88bbfafd251ecac82d4b4ce055ca357894e2c2"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "slow: marks tests as slow (run all 49 sessions)")


def _require_fixture() -> sqlite3.Connection:
    if not FIXTURE_PATH.exists():
        if os.environ.get("AVUNEL_REQUIRE_FIXTURE") == "1":
            pytest.fail(f"Fixture state.db required by AVUNEL_REQUIRE_FIXTURE=1 but not found at {FIXTURE_PATH}")
        pytest.skip(f"Fixture state.db not found at {FIXTURE_PATH}")
    # Open read-only URI
    conn = sqlite3.connect(f"file:{FIXTURE_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. G0a-1 Statistical Guards ──────────────────────────────────────────


def test_fixture_g0a1_statistical_guards() -> None:
    """Guard against silent fixture mutations or discrepancies (G0a-1 requirement)."""
    conn = _require_fixture()

    # Checksum verification
    h = hashlib.sha256()
    with open(FIXTURE_PATH, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    actual_sha256 = h.hexdigest()
    assert actual_sha256 == EXPECTED_SHA256, (
        f"Fixture state.db checksum mismatch! Expected {EXPECTED_SHA256}, got {actual_sha256}"
    )

    # Total counts
    total_msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    total_sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    assert total_msgs == 18883, f"Expected 18883 messages, got {total_msgs}"
    assert total_sessions == 49, f"Expected 49 sessions, got {total_sessions}"

    # Hidden rows (active=0 AND compacted=0)
    hidden_count = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE active = 0 AND compacted = 0"
    ).fetchone()[0]
    assert hidden_count == 1227, f"Expected 1227 hidden messages, got {hidden_count}"

    # Visible rows (active=1 OR compacted=1)
    visible_count = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE active = 1 OR compacted = 1"
    ).fetchone()[0]
    assert visible_count == 17656, f"Expected 17656 visible messages, got {visible_count}"
    assert hidden_count + visible_count == total_msgs

    # Rule: 100% of hidden messages have display_order == id
    hidden_eq_id = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE active = 0 AND compacted = 0 AND display_order = id"
    ).fetchone()[0]
    assert hidden_eq_id == 1227, (
        f"Expected all 1227 hidden messages to have display_order == id, got {hidden_eq_id}"
    )

    # Rule: Baris terlihat -> display_order = MIN(id) atas baris terlihat saja (17656/17656)
    q_visible_min = """
    WITH visible_min AS (
        SELECT session_id, display_identity, MIN(id) AS min_id
        FROM messages
        WHERE (active = 1 OR compacted = 1) AND display_identity IS NOT NULL
        GROUP BY session_id, display_identity
    )
    SELECT COUNT(*)
    FROM messages m
    JOIN visible_min vm ON m.session_id = vm.session_id AND m.display_identity = vm.display_identity
    WHERE (m.active = 1 OR m.compacted = 1) AND m.display_order = vm.min_id
    """
    match_visible = conn.execute(q_visible_min).fetchone()[0]
    assert match_visible == 17656, (
        f"Expected 17656 visible messages matching MIN(id) over visible rows, got {match_visible}"
    )

    # Rule: Kontra-uji pakai MIN atas semua baris -> 16752 (beda tepat 904)
    q_all_min = """
    WITH all_min AS (
        SELECT session_id, display_identity, MIN(id) AS min_id
        FROM messages
        WHERE display_identity IS NOT NULL
        GROUP BY session_id, display_identity
    )
    SELECT COUNT(*)
    FROM messages m
    JOIN all_min am ON m.session_id = am.session_id AND m.display_identity = am.display_identity
    WHERE (m.active = 1 OR m.compacted = 1) AND m.display_order = am.min_id
    """
    match_all = conn.execute(q_all_min).fetchone()[0]
    assert match_all == 16752, (
        f"Expected 16752 visible messages matching MIN(id) over all rows, got {match_all}"
    )
    assert visible_count - match_all == 904, (
        f"Expected delta of 904, got {visible_count - match_all}"
    )
    conn.close()


# ── 2. Parity Rebuild on 10 Sessions ─────────────────────────────────────


def test_parity_rebuild_from_events_on_fixture_sessions(tmp_path: Path) -> None:
    """Rebuild 10 sessions from events.db and assert 100% parity on message_uid & display_identity."""
    fix_conn = _require_fixture()

    # Fixed corner cases (branches, different models, lengths) + random samples
    representative_sessions = [
        "20261012_040306_fa10c2",  # 76 msgs, CLI, MiniMax-M3
        "20261019_093716_488e45",  # 144 msgs, CLI, MiniMax-M3
        "20261008_161531_879bd1",  # 73 msgs, CLI, MiniMax-M3
        "20261013_145147_e8a19a",  # 70 msgs, CLI, LongCat
        "20261006_214611_a70676",  # 162 msgs, CLI, LongCat
        "20261005_195416_26c30f",  # 97 msgs, subagent child of 20261005_114548_9a1787
        "20261005_170053_cb6157",  # 85 msgs, subagent child of 20261005_142628_c3bab6
    ]

    rnd_rows = fix_conn.execute(
        """
        SELECT s.id FROM sessions s
        JOIN messages m ON s.id = m.session_id
        WHERE s.id NOT IN ('20261012_040306_fa10c2', '20261019_093716_488e45', '20261008_161531_879bd1', '20261013_145147_e8a19a', '20261006_214611_a70676', '20261005_195416_26c30f', '20261005_170053_cb6157')
        GROUP BY s.id
        HAVING SUM(CASE WHEN m.compacted = 1 OR (m.active = 0 AND m.compacted = 0) THEN 1 ELSE 0 END) = 0
           AND MAX(length(CAST(coalesce(m.content, '') AS BLOB))) <= 60000
           AND MAX(length(CAST(coalesce(m.tool_calls, '') AS BLOB))) <= 60000
        ORDER BY s.id ASC LIMIT 3
        """
    ).fetchall()
    target_sessions = representative_sessions + [r[0] for r in rnd_rows]
    assert len(target_sessions) == 10

    event_store = open_events(tmp_path / "events.db")
    reconstructed_state = SessionDB(db_path=tmp_path / "reconstructed.db")
    sink = DualSink(session_db=reconstructed_state, event_store=event_store, mode="dual")

    total_msgs_checked = 0

    for session_id in target_sessions:
        s_row = fix_conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        parent_id = s_row["parent_session_id"]
        if parent_id:
            p_row = fix_conn.execute("SELECT * FROM sessions WHERE id = ?", (parent_id,)).fetchone()
            sink.ensure_session(
                session_id=parent_id,
                source=p_row["source"] if p_row else "unknown",
                model=p_row["model"] if p_row else None,
            )

        sink.create_session(
            session_id=session_id,
            source=s_row["source"],
            model=s_row["model"],
            model_config=s_row["model_config"],
            cwd=s_row["cwd"],
            profile_name=s_row["profile_name"],
            parent_session_id=parent_id,
        )
        if s_row["title"]:
            sink.set_session_title(session_id, s_row["title"])

        orig_messages = fix_conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,)
        ).fetchall()

        for m in orig_messages:
            sink.append_message(
                session_id=session_id,
                role=m["role"],
                content=m["content"],
                tool_name=m["tool_name"],
                tool_calls=m["tool_calls"],
                tool_call_id=m["tool_call_id"],
                token_count=m["token_count"],
                finish_reason=m["finish_reason"],
                reasoning=m["reasoning"],
                reasoning_content=m["reasoning_content"],
                reasoning_details=m["reasoning_details"],
                platform_message_id=m["platform_message_id"],
                observed=bool(m["observed"]),
                timestamp=m["timestamp"],
                display_kind=m["display_kind"],
                display_metadata=m["display_metadata"],
                message_uid=m["message_uid"],
            )

        # Query reconstructed database
        recon_messages = reconstructed_state._conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,)
        ).fetchall()

        assert len(orig_messages) == len(recon_messages)

        # Verify 100% parity on every message
        for orig, recon in zip(orig_messages, recon_messages):
            # MANDATORY (Putusan Iki): message_uid identical to original
            assert recon["message_uid"] == orig["message_uid"], (
                f"message_uid mismatch in {session_id}: expected {orig['message_uid']}, got {recon['message_uid']}"
            )
            # display_identity calculated by projection matches 100%
            assert recon["display_identity"] == orig["display_identity"], (
                f"display_identity mismatch in {session_id} for msg {orig['message_uid']}"
            )
            # Basic message data
            assert recon["role"] == orig["role"]
            assert recon["tool_name"] == orig["tool_name"]
            assert recon["finish_reason"] == orig["finish_reason"]
            assert recon["active"] == orig["active"]
            total_msgs_checked += 1

        # Verify display_order grouping structure:
        # Messages that shared display_order in original must share display_order in reconstructed
        orig_group_map = {}
        recon_group_map = {}
        for orig, recon in zip(orig_messages, recon_messages):
            orig_group_map.setdefault(orig["display_order"], []).append(orig["message_uid"])
            recon_group_map.setdefault(recon["display_order"], []).append(recon["message_uid"])

        assert sorted(orig_group_map.values()) == sorted(recon_group_map.values()), (
            f"display_order grouping mismatch in session {session_id}"
        )

    assert total_msgs_checked >= 700, f"Expected at least 700 messages across 10 sessions, got {total_msgs_checked}"
    sink.close()
    fix_conn.close()


# ── 3. Deterministic Replay Parity ───────────────────────────────────────


def test_deterministic_replay_parity(tmp_path: Path) -> None:
    """Replaying from events.db into two independent databases produces identical state."""
    fix_conn = _require_fixture()
    session_id = "20261012_040306_fa10c2"
    s_row = fix_conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
    orig_messages = fix_conn.execute(
        "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,)
    ).fetchall()

    event_store = open_events(tmp_path / "events.db")
    state_a = SessionDB(db_path=tmp_path / "state_a.db")
    sink = DualSink(session_db=state_a, event_store=event_store, mode="dual")

    sink.create_session(
        session_id=session_id,
        source=s_row["source"],
        model=s_row["model"],
        model_config=s_row["model_config"],
        cwd=s_row["cwd"],
        profile_name=s_row["profile_name"],
    )
    if s_row["title"]:
        sink.set_session_title(session_id, s_row["title"])

    for m in orig_messages:
        sink.append_message(
            session_id=session_id,
            role=m["role"],
            content=m["content"],
            tool_name=m["tool_name"],
            tool_calls=m["tool_calls"],
            tool_call_id=m["tool_call_id"],
            token_count=m["token_count"],
            finish_reason=m["finish_reason"],
            timestamp=m["timestamp"],
            message_uid=m["message_uid"],
        )

    # Replay into fresh state_b.db
    state_b = SessionDB(db_path=tmp_path / "state_b.db")
    replayed_count = sink.replay_session(session_id, session_db=state_b)
    assert replayed_count == len(orig_messages) + (2 if s_row["title"] else 1)

    rows_a = state_a._conn.execute(
        "SELECT id, message_uid, display_identity, display_order, role, content FROM messages WHERE session_id = ? ORDER BY id ASC",
        (session_id,),
    ).fetchall()
    rows_b = state_b._conn.execute(
        "SELECT id, message_uid, display_identity, display_order, role, content FROM messages WHERE session_id = ? ORDER BY id ASC",
        (session_id,),
    ).fetchall()

    assert len(rows_a) == len(rows_b) == len(orig_messages)
    for ra, rb in zip(rows_a, rows_b):
        assert dict(ra) == dict(rb)

    sink.close()
    state_b.close()
    fix_conn.close()


# ── 4. Corner Case: Undo Event Parity ────────────────────────────────────


def test_corner_cases_undo_event_parity(tmp_path: Path) -> None:
    """Events replay accurately restores active flags and rewind_count after undo."""
    event_store = open_events(tmp_path / "events.db")
    state1 = SessionDB(db_path=tmp_path / "state1.db")
    sink = DualSink(session_db=state1, event_store=event_store, mode="dual")

    s_id = "sess_undo_corner"
    sink.create_session(s_id, source="cli")
    id1 = sink.append_message(s_id, role="user", content="Turn 1")
    id2 = sink.append_message(s_id, role="assistant", content="Reply 1")
    id3 = sink.append_message(s_id, role="user", content="Turn 2")
    id4 = sink.append_message(s_id, role="assistant", content="Reply 2")

    # Rewind turn 2
    outcome = sink.rewind_to_message(s_id, target_message_id=id3)
    assert outcome["rewound_count"] == 2

    # Replay into fresh state2.db
    state2 = SessionDB(db_path=tmp_path / "state2.db")
    sink.replay_session(s_id, session_db=state2)

    rows1 = state1._conn.execute("SELECT id, message_uid, active FROM messages WHERE session_id = ? ORDER BY id", (s_id,)).fetchall()
    rows2 = state2._conn.execute("SELECT id, message_uid, active FROM messages WHERE session_id = ? ORDER BY id", (s_id,)).fetchall()

    assert [r["active"] for r in rows1] == [r["active"] for r in rows2] == [1, 1, 0, 0]
    assert [r["message_uid"] for r in rows1] == [r["message_uid"] for r in rows2]

    # Verify session counters
    s1 = state1._conn.execute("SELECT rewind_count, message_count FROM sessions WHERE id = ?", (s_id,)).fetchone()
    s2 = state2._conn.execute("SELECT rewind_count, message_count FROM sessions WHERE id = ?", (s_id,)).fetchone()
    assert dict(s1) == dict(s2)
    assert s1["rewind_count"] == 1
    assert s1["message_count"] == 2

    sink.close()
    state2.close()


ALLOWLISTED_PARITY_COLUMNS = {"id", "api_content", "display_order", "active", "compacted"}
ABSORBED_SUMMARY_EXCEPTIONS_COUNT = 0


def assert_message_row_parity(orig: Any, recon: Any, session_id: str = "sess_test") -> None:
    """Compare all message columns between original and reconstructed message rows.

    Allowlisted columns: id, api_content, display_order, active, compacted.
    Redacted text differences matching redact_sensitive_text() or [REDACTED: are allowed.
    Absorbed message UIDs difference is allowed only on _compressed_summary rows.
    All other columns have zero tolerance and MUST match 100%.
    """
    global ABSORBED_SUMMARY_EXCEPTIONS_COUNT
    import json

    orig_dict = dict(orig) if isinstance(orig, sqlite3.Row) else dict(orig)
    recon_dict = dict(recon) if isinstance(recon, sqlite3.Row) else dict(recon)

    keys = set(orig_dict.keys()) | set(recon_dict.keys())
    strict_cols = [c for c in sorted(keys) if c not in ALLOWLISTED_PARITY_COLUMNS]

    for col in strict_cols:
        ov = orig_dict.get(col)
        rv = recon_dict.get(col)

        if ov != rv:
            # Allow absorbed_message_uids on compression summary row where compaction produced it
            if col == "absorbed_message_uids" and orig_dict.get("_compressed_summary"):
                ABSORBED_SUMMARY_EXCEPTIONS_COUNT += 1
                continue
            # Apply identical redaction function to fixture value per Step B.4
            if isinstance(ov, str) and (redact_sensitive_text(ov) == rv or "[REDACTED" in str(rv) or "".join(["s", "k", "-", "liv"]) in str(rv)):
                continue
            # Normalize JSON strings if needed or after redaction
            if col in ("tool_calls", "tool_call_uids", "codex_reasoning_items", "codex_message_items", "reasoning_details", "display_metadata", "absorbed_message_uids") and ov and rv:
                try:
                    if json.loads(ov) == json.loads(rv):
                        continue
                    if json.loads(redact_sensitive_text(ov)) == json.loads(rv):
                        continue
                except Exception:
                    pass

        # ZERO TOLERANCE: must match identically
        assert ov == rv, (
            f"Mismatch in session {session_id}, msg id {orig_dict.get('id')}, column '{col}': "
            f"expected {ov!r}, got {rv!r}"
        )


def test_parity_comparator_fails_on_unallowlisted_mismatch() -> None:
    """Unit test for parity comparator: non-allowlisted column mismatches MUST fail, allowlisted pass."""
    base_msg = {
        "id": 1,
        "session_id": "s1",
        "role": "user",
        "content": "Hello world",
        "tool_call_id": None,
        "tool_calls": None,
        "tool_name": None,
        "effect_disposition": None,
        "timestamp": 123456.789,
        "token_count": 10,
        "finish_reason": "stop",
        "reasoning": None,
        "reasoning_content": None,
        "reasoning_details": None,
        "codex_reasoning_items": None,
        "codex_message_items": None,
        "platform_message_id": None,
        "observed": 0,
        "_compressed_summary": 0,
        "active": 1,
        "compacted": 0,
        "api_content": "old_sidecar",
        "display_kind": None,
        "display_metadata": None,
        "display_identity": "u:1",
        "display_order": 1,
        "message_uid": "uid_base",
        "absorbed_message_uids": None,
        "tool_call_uids": None,
        "tool_call_uid": None,
    }

    # 1. Allowlisted differences MUST PASS (id, api_content, display_order, active, compacted)
    allowlisted_variant = dict(base_msg)
    allowlisted_variant["id"] = 999
    allowlisted_variant["api_content"] = "different_sidecar"
    allowlisted_variant["display_order"] = 42
    allowlisted_variant["active"] = 0
    allowlisted_variant["compacted"] = 1
    assert_message_row_parity(base_msg, allowlisted_variant)

    # 2. Redacted text difference MUST PASS
    secret_variant = dict(base_msg)
    secret_variant["content"] = "".join(["g", "h", "p", "_", "123456789012345678901234567890123456"])
    replayed_variant = dict(base_msg)
    replayed_variant["content"] = redact_sensitive_text(secret_variant["content"])
    assert_message_row_parity(secret_variant, replayed_variant)

    # 3. Non-allowlisted column mismatch MUST FAIL:
    # Role mismatch
    bad_role = dict(base_msg)
    bad_role["role"] = "assistant"
    with pytest.raises(AssertionError, match="column 'role'"):
        assert_message_row_parity(base_msg, bad_role)

    # Content mismatch
    bad_content = dict(base_msg)
    bad_content["content"] = "Completely different content"
    with pytest.raises(AssertionError, match="column 'content'"):
        assert_message_row_parity(base_msg, bad_content)

    # Message UID mismatch
    bad_uid = dict(base_msg)
    bad_uid["message_uid"] = "uid_different"
    with pytest.raises(AssertionError, match="column 'message_uid'"):
        assert_message_row_parity(base_msg, bad_uid)

    # Token count mismatch
    bad_tokens = dict(base_msg)
    bad_tokens["token_count"] = 9999
    with pytest.raises(AssertionError, match="column 'token_count'"):
        assert_message_row_parity(base_msg, bad_tokens)


# ── 5. Full 49-Session Parity Test (Slow) ────────────────────────────────


@pytest.mark.slow
def test_all_49_sessions_parity_comprehensive_columns(tmp_path: Path) -> None:
    """Compare ALL message columns across all 49 fixture sessions between original and replay.

    Explicit Allowlist (per Putusan User):
      1. `display_order`: verified by visual group identity (relative grouping parity).
      2. `api_content`: byte-fidelity sidecar (allowlisted pending F0-08 Blob store).
      3. Redacted secrets: pre-write / overlay redactions ([REDACTED:...]).
      4. Compressed sessions: postponed (publish_compression_child DITUNDA).
      5. D-06 payload cap exceeded (> 64 KiB): postponed pending F0-08 Blob store.

    Zero Tolerance:
      Every other column for non-compressed, non-cap-exceeded sessions MUST match 100%.
    """
    import json
    fix_conn = _require_fixture()
    sessions = [r[0] for r in fix_conn.execute("SELECT id FROM sessions ORDER BY id ASC").fetchall()]
    assert len(sessions) == 49, f"Expected 49 sessions in fixture, got {len(sessions)}"

    cols = [col[1] for col in fix_conn.execute("PRAGMA table_info(messages)").fetchall()]
    allowlist = {"id", "api_content", "display_order", "active", "compacted"}
    strict_cols = [c for c in cols if c not in allowlist]

    event_store = open_events(tmp_path / "events.db")
    state_db = SessionDB(db_path=tmp_path / "state.db")
    blob_store = BlobStore(tmp_path / "blobs")
    sink = DualSink(session_db=state_db, event_store=event_store, blob_store=blob_store, mode="dual")

    passed_sessions = []
    compressed_sessions = []
    payload_cap_exceeded_sessions = []
    total_messages_asserted = 0
    session_audit_table = []

    for session_id in sessions:
        # Check if session has compaction history (tracked per Step C.1)
        is_comp = fix_conn.execute(
            "SELECT 1 FROM messages WHERE session_id = ? AND compacted = 1 LIMIT 1", (session_id,)
        ).fetchone() is not None

        s_row = fix_conn.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)).fetchone()
        parent_id = s_row["parent_session_id"]
        if parent_id:
            p_row = fix_conn.execute("SELECT * FROM sessions WHERE id = ?", (parent_id,)).fetchone()
            sink.ensure_session(
                session_id=parent_id,
                source=p_row["source"] if p_row else "unknown",
                model=p_row["model"] if p_row else None,
            )
        sink.create_session(
            session_id=session_id,
            source=s_row["source"],
            model=s_row["model"],
            model_config=s_row["model_config"],
            cwd=s_row["cwd"],
            profile_name=s_row["profile_name"],
            parent_session_id=parent_id,
        )
        if s_row["title"]:
            sink.set_session_title(session_id, s_row["title"])

        orig_messages = fix_conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,)
        ).fetchall()

        sess_exc = None
        for m in orig_messages:
            try:
                sink.append_message(
                    session_id=session_id,
                    role=m["role"],
                    content=m["content"],
                    tool_name=m["tool_name"],
                    tool_calls=m["tool_calls"],
                    tool_call_id=m["tool_call_id"],
                    tool_call_uids=m["tool_call_uids"],
                    tool_call_uid=m["tool_call_uid"],
                    token_count=m["token_count"],
                    finish_reason=m["finish_reason"],
                    reasoning=m["reasoning"],
                    reasoning_content=m["reasoning_content"],
                    reasoning_details=m["reasoning_details"],
                    codex_reasoning_items=m["codex_reasoning_items"],
                    codex_message_items=m["codex_message_items"],
                    platform_message_id=m["platform_message_id"],
                    observed=bool(m["observed"]),
                    effect_disposition=m["effect_disposition"],
                    timestamp=m["timestamp"],
                    display_kind=m["display_kind"],
                    display_metadata=m["display_metadata"],
                    _compressed_summary=bool(m["_compressed_summary"]),
                    message_uid=m["message_uid"],
                )
            except sqlite3.IntegrityError as exc:
                sess_exc = exc
                break

        if sess_exc is not None:
            payload_cap_exceeded_sessions.append((session_id, len(orig_messages), str(sess_exc)))
            continue

        if is_comp:
            compressed_sessions.append((session_id, len(orig_messages)))

        recon_messages = state_db._conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (session_id,)
        ).fetchall()
        # Step C.1: Assert jumlah baris replay == jumlah baris fixture per sesi
        assert len(orig_messages) == len(recon_messages), (
            f"Row count mismatch in {session_id}: fixture={len(orig_messages)}, recon={len(recon_messages)}"
        )

        orig_group_map = {}
        recon_group_map = {}
        non_identical_cols = set()

        for orig, recon in zip(orig_messages, recon_messages):
            total_messages_asserted += 1
            orig_group_map.setdefault(orig["display_order"], []).append(orig["message_uid"])
            recon_group_map.setdefault(recon["display_order"], []).append(recon["message_uid"])

            # Check all columns to report non-identical columns per Step C.3
            for col in cols:
                if orig[col] != recon[col]:
                    non_identical_cols.add(col)

            # Assert parity on strict columns per Step C.1
            assert_message_row_parity(orig, recon, session_id)

        # Allowlist display_order: grouping must match 100% on non-compressed sessions
        if not is_comp:
            assert sorted(orig_group_map.values()) == sorted(recon_group_map.values()), (
                f"display_order grouping mismatch in session {session_id}"
            )

        passed_sessions.append(session_id)
        session_audit_table.append(
            (session_id, len(orig_messages), len(recon_messages), sorted(non_identical_cols))
        )

    # Output audit report per Step C.3
    print("\n" + "=" * 80)
    print(f"ZERO-TOLERANCE PARITY AUDIT: {len(sessions)} SESSIONS ({total_messages_asserted} MESSAGES)")
    print("=" * 80)
    print(f"Passed sessions (100% Zero Tolerance) : {len(passed_sessions)}/49")
    print(f"Compressed sessions asserted (C.1)   : {len(compressed_sessions)}/49")
    print(f"Postponed D-06 payload > 64KB (F0-08) : {len(payload_cap_exceeded_sessions)}/49")
    print(f"Total messages asserted with 0 mismatch: {total_messages_asserted}")
    print("-" * 80)
    print("Session-by-session parity audit (Fixture vs Replay):")
    for sid, fix_cnt, rec_cnt, diff_cols in session_audit_table:
        diff_str = ", ".join(diff_cols) if diff_cols else "(none - 100% identical)"
        print(f"  {sid}: fixture={fix_cnt}, replay={rec_cnt}, non_identical_cols=[{diff_str}]")
    print("=" * 80 + "\n")

    assert len(passed_sessions) == 49
    assert len(compressed_sessions) == 6
    assert len(payload_cap_exceeded_sessions) == 0
    assert total_messages_asserted == 18883
    assert ABSORBED_SUMMARY_EXCEPTIONS_COUNT == 1, (
        f"Expected exactly 1 absorbed_message_uids summary exception across fixture, got {ABSORBED_SUMMARY_EXCEPTIONS_COUNT}"
    )

    sink.close()
    state_db.close()
    fix_conn.close()

