"""Tests for Avunel Blob Store (F0-08 / D-10).

Verifies:
- Content-addressable storage via SHA-256 ("sha256:<64-hex>").
- Deduplicated, atomic writes via temp file and os.replace.
- Directory sharding (<root>/<first2>/<rest62>).
- Streaming open, get, get_text, size, exists, delete with shard cleanup.
- Line range reading (read_range) and regex search (grep) for F3-09 prep.
- Input validation and directory traversal prevention.
- Helpers: store_api_content and load_api_content.
- Integration with EventStore (api_content_ref column).
- Integration with DualSink (dual-write and projection replay parity for api_content).
- Large payload overflow handling (> 64 KiB) per D-06.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import tempfile
import time
import pytest

from blob_store import (
    BlobStore,
    compute_sha256,
    open_blob_store,
    store_api_content,
    load_api_content,
    offload_payload_if_needed,
    restore_payload_fields,
    SNIPPET_MAX_BYTES,
    PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
)
from dual_sink import DualSink
from hermes_state import SessionDB
from hermes_state_events import EventStore, open_events


def test_compute_sha256_known_vectors() -> None:
    """compute_sha256 returns standard RFC 6234 hex digests."""
    assert compute_sha256(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert (
        compute_sha256(b"hello world")
        == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )


def test_blob_store_init_and_sharded_paths(tmp_path: Path) -> None:
    """BlobStore creates root directory and computes 2-level sharded paths."""
    root = tmp_path / "custom_blobs"
    assert not root.exists()

    store = BlobStore(root)
    assert root.exists()
    assert store.root_dir == root.resolve()

    digest = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    path = store._path_for_digest(digest)
    expected_path = root / "b9" / "4d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    assert path == expected_path


def test_blob_store_put_get_bytes_and_string(tmp_path: Path) -> None:
    """BlobStore handles bytes and str content, sizes, and existence checks."""
    store = BlobStore(tmp_path / "blobs")

    # 1. Store bytes
    raw_data = b"binary content \x00\xff\xfe"
    ref_bytes = store.put(raw_data)
    expected_digest = compute_sha256(raw_data)
    assert ref_bytes == f"sha256:{expected_digest}"
    assert store.exists(ref_bytes)
    assert store.size(ref_bytes) == len(raw_data)
    assert store.get(ref_bytes) == raw_data

    # 2. Store str
    text_data = "Hello, Avunel event log! 🌟"
    ref_str = store.put(text_data)
    expected_str_digest = compute_sha256(text_data.encode("utf-8"))
    assert ref_str == f"sha256:{expected_str_digest}"
    assert store.exists(ref_str)
    assert store.size(ref_str) == len(text_data.encode("utf-8"))
    assert store.get_text(ref_str) == text_data
    assert store.get(ref_str) == text_data.encode("utf-8")


def test_blob_store_deduplication_and_atomicity(tmp_path: Path) -> None:
    """Subsequent puts of identical content are deduplicated without leftover temp files."""
    store = BlobStore(tmp_path / "blobs")
    data = "Identical repeated data content for deduplication test"

    ref1 = store.put(data)
    ref2 = store.put(data)
    ref3 = store.put(data.encode("utf-8"))

    assert ref1 == ref2 == ref3

    digest = ref1.split(":")[1]
    shard_dir = store.root_dir / digest[:2]
    # Check shard directory contains exactly 1 blob file and 0 temporary files
    files = list(shard_dir.iterdir())
    assert len(files) == 1
    assert files[0].name == digest[2:]
    assert not any(f.name.startswith(".tmp_blob_") for f in files)


def test_blob_store_streaming_open(tmp_path: Path) -> None:
    """open() returns a readable binary stream matching the blob content."""
    store = BlobStore(tmp_path / "blobs")
    content = b"Streamable payload " * 100
    ref = store.put(content)

    with store.open(ref) as stream:
        read_back = stream.read()
        assert read_back == content


def test_blob_store_security_and_traversal_rejection(tmp_path: Path) -> None:
    """Malformed refs and path traversal payloads are strictly rejected."""
    store = BlobStore(tmp_path / "blobs")

    # Non-string
    with pytest.raises(TypeError, match="Blob ref must be a string"):
        store.get(12345)  # type: ignore[arg-type]

    # Missing sha256: prefix
    with pytest.raises(ValueError, match="Invalid blob ref format"):
        store.get("md5:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")

    # Invalid digest length
    with pytest.raises(ValueError, match="Invalid SHA-256 digest"):
        store.get("sha256:abcd")

    # Path traversal attempts
    with pytest.raises(ValueError, match="Invalid SHA-256 digest"):
        store.get("sha256:../../etc/passwd")

    with pytest.raises(ValueError, match="Invalid SHA-256 digest"):
        store.get("sha256:b9/../../foo/bar/baz12345678901234567890123456789012345678901234")

    with pytest.raises(ValueError, match="Invalid SHA-256 digest"):
        store.get("sha256:" + "a" * 62 + "..")

    # exists() returns False safely for invalid refs without raising
    assert not store.exists("invalid_ref")
    assert not store.exists("sha256:short")
    assert not store.exists("sha256:../../etc/passwd")

    # Missing blob raises KeyError
    missing_ref = "sha256:" + "0" * 64
    with pytest.raises(KeyError, match="Blob not found"):
        store.get(missing_ref)

    with pytest.raises(KeyError, match="Blob not found"):
        store.size(missing_ref)

    with pytest.raises(KeyError, match="Blob not found"):
        store.open(missing_ref)


def test_blob_store_read_range(tmp_path: Path) -> None:
    """read_range extracts line slices using 1-indexed line numbers."""
    store = BlobStore(tmp_path / "blobs")
    content = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5\n"
    ref = store.put(content)

    # Specific range
    assert store.read_range(ref, start_line=1, end_line=3) == "Line 1\nLine 2\nLine 3\n"
    assert store.read_range(ref, start_line=2, end_line=4) == "Line 2\nLine 3\nLine 4\n"
    assert store.read_range(ref, start_line=3, end_line=3) == "Line 3\n"

    # From start / to end
    assert store.read_range(ref, start_line=4) == "Line 4\nLine 5\n"
    assert store.read_range(ref, end_line=2) == "Line 1\nLine 2\n"

    # Full file
    assert store.read_range(ref) == content

    # Out of range boundaries
    assert store.read_range(ref, start_line=10, end_line=20) == ""
    assert store.read_range(ref, start_line=-5, end_line=2) == "Line 1\nLine 2\n"


def test_blob_store_grep(tmp_path: Path) -> None:
    """grep searches regex pattern line-by-line and returns (line_number, text)."""
    store = BlobStore(tmp_path / "blobs")
    content = (
        "INFO: system initialized\n"
        "DEBUG: loading plugins\n"
        "ERROR: connection timeout to server A\n"
        "INFO: heartbeat ok\n"
        "ERROR: connection timeout to server B\n"
        "WARN: high memory usage\n"
    )
    ref = store.put(content)

    matches = store.grep(ref, r"ERROR: connection timeout to (.*)")
    assert len(matches) == 2
    assert matches[0] == (3, "ERROR: connection timeout to server A")
    assert matches[1] == (5, "ERROR: connection timeout to server B")

    # max_matches limit
    capped = store.grep(ref, r"ERROR:", max_matches=1)
    assert len(capped) == 1
    assert capped[0][0] == 3


def test_blob_store_delete_and_shard_cleanup(tmp_path: Path) -> None:
    """delete removes blob and prunes empty shard directory."""
    store = BlobStore(tmp_path / "blobs")
    ref = store.put("temporary discardable blob")
    digest = ref.split(":")[1]
    shard_dir = store.root_dir / digest[:2]

    assert store.exists(ref)
    assert shard_dir.exists()

    # Deleting existing blob
    assert store.delete(ref) is True
    assert not store.exists(ref)
    # Shard directory should be removed because it became empty
    assert not shard_dir.exists()

    # Deleting already-deleted / non-existent blob
    assert store.delete(ref) is False


def test_global_helpers_store_and_load_api_content(tmp_path: Path) -> None:
    """store_api_content and load_api_content integrate with BlobStore."""
    store = open_blob_store(tmp_path / "blobs")

    # None handling
    assert store_api_content(None, blob_store=store) is None
    assert load_api_content(None, blob_store=store) is None
    assert load_api_content("invalid_ref", blob_store=store) is None
    assert load_api_content("sha256:" + "f" * 64, blob_store=store) is None

    # Normal round-trip
    sidecar_text = "<system>Specialized raw provider prompt</system>"
    ref = store_api_content(sidecar_text, blob_store=store)
    assert ref is not None
    assert ref.startswith("sha256:")

    retrieved = load_api_content(ref, blob_store=store)
    assert retrieved == sidecar_text


def test_event_store_integration_with_api_content_ref(tmp_path: Path) -> None:
    """EventStore accepts and preserves api_content_ref for message.* events."""
    events_db = tmp_path / "events.db"
    blobs_dir = tmp_path / "blobs"
    blob_store = BlobStore(blobs_dir)
    event_store = open_events(events_db)

    # 1. Put raw provider prompt in blob store
    raw_api_bytes = "Exact prompt bytes sent to provider API with cache keys"
    api_ref = store_api_content(raw_api_bytes, blob_store=blob_store)
    assert api_ref is not None

    # 2. Emit message.appended event referencing the blob
    event_id = event_store.emit(
        type="message.appended",
        ts=int(time.time() * 1000),
        session_id="session_f0_08",
        actor="session:session_f0_08",
        payload={
            "session_id": "session_f0_08",
            "role": "assistant",
            "content": "Hello, user!",
            "message_uid": "msg_uid_12345678",
        },
        api_content_ref=api_ref,
    )
    assert event_id > 0

    # 3. Read back event from events.db
    ev = event_store.session_events("session_f0_08")[0]
    assert ev["api_content_ref"] == api_ref

    # 4. Resolve content from blob store
    loaded_content = load_api_content(ev["api_content_ref"], blob_store=blob_store)
    assert loaded_content == raw_api_bytes


def test_dual_sink_stores_and_projects_api_content(tmp_path: Path) -> None:
    """DualSink automatically offloads api_content to blob store and restores it on projection replay."""
    state_db_path = tmp_path / "state.db"
    events_db_path = tmp_path / "events.db"
    blobs_dir = tmp_path / "blobs"

    session_db = SessionDB(state_db_path)
    event_store = open_events(events_db_path)
    blob_store = BlobStore(blobs_dir)

    sink = DualSink(
        session_db=session_db,
        event_store=event_store,
        blob_store=blob_store,
        mode="dual",
    )

    s_id = "sess_dual_api_content"
    sink.create_session(s_id, source="test")

    # Append message with api_content
    raw_sidecar = "Raw byte-fidelity sidecar string sent to LLM provider"
    sink.append_message(
        session_id=s_id,
        role="assistant",
        content="Normal readable assistant response",
        api_content=raw_sidecar,
    )

    # 1. Check events.db: api_content_ref is a sha256 reference
    events = event_store.session_events(s_id)
    msg_events = [e for e in events if e["type"] == "message.appended"]
    assert len(msg_events) == 1
    ev = msg_events[0]
    api_ref = ev["api_content_ref"]
    assert api_ref is not None
    assert api_ref.startswith("sha256:")

    # Check blob store contains the sidecar
    assert blob_store.exists(api_ref)
    assert blob_store.get_text(api_ref) == raw_sidecar

    # 2. Check state.db: api_content was written
    msg_state = session_db.get_messages(s_id)[0]
    assert msg_state["content"] == "Normal readable assistant response"
    assert msg_state["api_content"] == raw_sidecar

    # 3. Replay from events.db into a fresh SessionDB projection
    fresh_state_path = tmp_path / "fresh_state.db"
    fresh_session_db = SessionDB(fresh_state_path)

    projected_count = sink.replay_session(s_id, session_db=fresh_session_db)
    assert projected_count == 2  # session.created + message.appended

    # 4. Verify 100% parity of api_content in projected SessionDB
    fresh_msg = fresh_session_db.get_messages(s_id)[0]
    assert fresh_msg["content"] == "Normal readable assistant response"
    assert fresh_msg["api_content"] == raw_sidecar
    assert fresh_msg["message_uid"] == msg_state["message_uid"]

    sink.close()
    fresh_session_db.close()


def test_large_payload_overflow_handling_via_blob_store(tmp_path: Path) -> None:
    """Large content exceeding D-06 64 KiB cap is offloaded to blob store."""
    events_db_path = tmp_path / "events.db"
    blobs_dir = tmp_path / "blobs"

    event_store = open_events(events_db_path)
    blob_store = BlobStore(blobs_dir)

    # Create 128 KiB payload (> 64 KiB D-06 limit)
    large_tool_output = "Line with data: " + ("x" * 128) + "\n"
    large_tool_output = large_tool_output * 1000
    assert len(large_tool_output.encode("utf-8")) > 100 * 1024  # > 100 KiB

    # Offload large body to blob store
    blob_ref = blob_store.put(large_tool_output)

    # Event payload only contains metadata and blob reference
    lightweight_payload = {
        "session_id": "sess_large_payload",
        "role": "tool",
        "tool_name": "run_command",
        "content_blob_ref": blob_ref,
        "content_size": blob_store.size(blob_ref),
        "message_uid": "msg_uid_large_output",
    }

    event_id = event_store.emit(
        type="message.appended",
        ts=int(time.time() * 1000),
        session_id="sess_large_payload",
        actor="tool:run_command",
        payload=lightweight_payload,
        api_content_ref=blob_ref,
    )
    assert event_id > 0

    # Retrieve and verify full content from blob store
    ev = event_store.session_events("sess_large_payload")[0]
    assert ev["api_content_ref"] == blob_ref
    assert blob_store.get_text(ev["api_content_ref"]) == large_tool_output


def test_blob_store_permissions_and_directory_fsync(tmp_path: Path) -> None:
    """root_dir and shard_dir have mode 0700; blob files have mode 0600."""
    blobs_dir = tmp_path / "secure_blobs"
    store = BlobStore(blobs_dir)

    # Check root_dir permission is 0700
    assert (store.root_dir.stat().st_mode & 0o777) == 0o700

    ref = store.put("sensitive payload data")
    digest = ref.split(":")[1]
    shard_dir = store.root_dir / digest[:2]
    blob_file = shard_dir / digest[2:]

    # Check shard_dir is 0700
    assert (shard_dir.stat().st_mode & 0o777) == 0o700
    # Check blob file is 0600 (owner read/write only)
    assert (blob_file.stat().st_mode & 0o777) == 0o600


def test_blob_store_delete_event_reference_guard(tmp_path: Path) -> None:
    """delete() refuses to delete blob if referenced by an event in event_store."""
    events_db = tmp_path / "events.db"
    blobs_dir = tmp_path / "blobs"
    event_store = open_events(events_db)
    blob_store = BlobStore(blobs_dir)

    ref = blob_store.put("critical shared event payload")
    assert blob_store.exists(ref)

    # 1. Emit event referencing this ref in api_content_ref
    event_store.emit(
        type="message.appended",
        ts=int(time.time() * 1000),
        session_id="sess_ref_guard",
        actor="session:sess_ref_guard",
        payload={"session_id": "sess_ref_guard", "role": "user", "message_uid": "uid_guard_1"},
        api_content_ref=ref,
    )

    # Refuse delete because event references ref
    assert blob_store.delete(ref, event_store=event_store) is False
    assert blob_store.exists(ref) is True

    # 2. Ref in payload_json
    ref2 = blob_store.put("large payload in json")
    event_store.emit(
        type="message.appended",
        ts=int(time.time() * 1000),
        session_id="sess_ref_guard",
        actor="session:sess_ref_guard",
        payload={
            "session_id": "sess_ref_guard",
            "role": "assistant",
            "message_uid": "uid_guard_2",
            "content_blob_ref": ref2,
        },
        api_content_ref="msg:uid_guard_2",
    )
    assert blob_store.delete(ref2, event_store=event_store) is False
    assert blob_store.exists(ref2) is True

    # 3. Unreferenced blob deletes normally
    unreferenced_ref = blob_store.put("unreferenced discardable data")
    assert blob_store.delete(unreferenced_ref, event_store=event_store) is True
    assert blob_store.exists(unreferenced_ref) is False


def test_offload_payload_if_needed_and_restore(tmp_path: Path) -> None:
    """Large payload fields are offloaded one by one to BlobStore with 2 KiB snippet & redaction."""
    blobs_dir = tmp_path / "blobs"
    blob_store = BlobStore(blobs_dir)

    # 1. Payload under threshold stays untouched
    small_payload = {
        "session_id": "sess_small",
        "role": "user",
        "content": "Short message",
        "message_uid": "uid_small",
    }
    offloaded_small = offload_payload_if_needed(small_payload, blob_store=blob_store, threshold_bytes=1000)
    assert offloaded_small == small_payload

    # 2. Large payload exceeds threshold: offload largest field
    token_str = "".join(["s", "k", "-", "live-1234567890abcdef"])
    giant_content = f"Secret token: {token_str}. " + ("A" * 70000)
    large_payload = {
        "session_id": "sess_large",
        "role": "assistant",
        "content": giant_content,
        "tool_name": "bash",
        "message_uid": "uid_large",
    }
    offloaded = offload_payload_if_needed(large_payload, blob_store=blob_store, threshold_bytes=1000)

    # Field content is offloaded
    assert offloaded["content"] is None
    blob_ref = offloaded["content_blob_ref"]
    assert blob_ref.startswith("sha256:")
    assert offloaded["content_size"] > 70000

    # Snippet is <= SNIPPET_MAX_BYTES and has secrets redacted
    snippet = offloaded["content_snippet"]
    assert len(snippet.encode("utf-8")) <= SNIPPET_MAX_BYTES
    assert token_str not in snippet
    assert "".join(["s", "k", "-", "liv...cdef"]) in snippet

    # Blob content on disk also has secrets redacted
    blob_content = blob_store.get_text(blob_ref)
    assert token_str not in blob_content
    assert "".join(["s", "k", "-", "liv...cdef"]) in blob_content

    # 3. Restore payload fields recovers the original string verbatim
    restored = restore_payload_fields(offloaded, blob_store=blob_store)
    assert restored["content"] == blob_content
    assert restored["message_uid"] == "uid_large"

