"""Tests for G0-Contract freezing: port, security, SSE formats, and Origin policy."""

from __future__ import annotations

import json
import pytest

from f0_contract import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    HEARTBEAT_INTERVAL_SECONDS,
    POLL_INTERVAL_SECONDS,
    format_sse_event,
    format_sse_heartbeat,
    is_origin_allowed,
    validate_host,
    validate_port,
    verify_token,
)


def test_frozen_constants() -> None:
    """Verify frozen port and default network bindings."""
    assert DEFAULT_PORT == 31032
    assert DEFAULT_HOST == "127.0.0.1"
    assert HEARTBEAT_INTERVAL_SECONDS == 15.0
    assert POLL_INTERVAL_SECONDS == 0.25


def test_validate_port_valid_and_invalid() -> None:
    """Port validation must accept valid ports and reject out of range."""
    assert validate_port(31032) == 31032
    assert validate_port("31032") == 31032
    assert validate_port(1024) == 1024
    assert validate_port(65535) == 65535

    with pytest.raises(ValueError, match="out of valid user range"):
        validate_port(80)  # Privileged port < 1024

    with pytest.raises(ValueError, match="out of valid user range"):
        validate_port(70000)  # > 65535

    with pytest.raises(ValueError, match="must be an integer"):
        validate_port("not_a_number")


def test_validate_host_loopback_only() -> None:
    """Server must reject non-loopback bindings."""
    assert validate_host("127.0.0.1") == "127.0.0.1"
    assert validate_host("localhost") == "localhost"

    with pytest.raises(ValueError, match="Security refusal"):
        validate_host("0.0.0.0")

    with pytest.raises(ValueError, match="Security refusal"):
        validate_host("192.168.1.50")


def test_verify_token_timing_safe() -> None:
    """Token verification must be timing-safe and reject empty/mismatched tokens."""
    expected = "super-secret-token-12345"
    assert verify_token(expected, expected) is True
    assert verify_token("wrong-token", expected) is False
    assert verify_token("", expected) is False
    assert verify_token(None, expected) is False
    assert verify_token(expected, "") is False


def test_is_origin_allowed() -> None:
    """Origin check must allow loopback and reject external origins."""
    port = 31032
    # Non-browser requests (no Origin header) are allowed
    assert is_origin_allowed(None, port=port) is True
    assert is_origin_allowed("", port=port) is True

    # Valid loopback origins
    assert is_origin_allowed(f"http://127.0.0.1:{port}", port=port) is True
    assert is_origin_allowed(f"http://localhost:{port}", port=port) is True

    # Invalid origins
    assert is_origin_allowed("http://malicious-site.com", port=port) is False
    assert is_origin_allowed("https://attacker.org:31032", port=port) is False
    assert is_origin_allowed(f"http://127.0.0.1:{port + 1}", port=port) is False


def test_format_sse_event_and_heartbeat() -> None:
    """SSE event and heartbeat formatting must match W3C standard with contract flags."""
    ev = {
        "id": 42,
        "type": "message.appended",
        "session_id": "sess_test",
        "actor": "cli:sess_test",
        "payload_json": json.dumps({"role": "user", "content": "test"}),
    }
    raw = format_sse_event(ev)
    assert raw.startswith("id: 42\n")
    assert "event: message.appended\n" in raw
    assert 'data: {"id": 42' in raw
    assert raw.endswith("\n\n")

    # Extract JSON data payload
    data_line = [l for l in raw.split("\n") if l.startswith("data: ")][0]
    payload = json.loads(data_line[6:])
    assert "is_redacted" in payload
    assert payload["is_redacted"] is False
    assert "has_blob" in payload
    assert payload["has_blob"] is False

    hb = format_sse_heartbeat(1730000000.0)
    assert hb.startswith(": heartbeat ")
    assert hb.endswith("\n\n")


def test_format_sse_redaction_and_blob_detection() -> None:
    """format_sse_event must set is_redacted=True and has_blob=True correctly."""
    # 1. Redacted secret event (real pattern: sk-liv...5678)
    ev_redacted = {
        "id": 101,
        "type": "message.appended",
        "session_id": "sess_secret",
        "actor": "cli:sess_secret",
        "payload_json": json.dumps({"role": "user", "content": "Check key sk-liv...5678"}),
    }
    raw_redacted = format_sse_event(ev_redacted)
    data_redacted = json.loads([l for l in raw_redacted.split("\n") if l.startswith("data: ")][0][6:])
    assert data_redacted["is_redacted"] is True
    assert data_redacted["has_blob"] is False

    # 2. Blob-offloaded event (>60 KiB or content_blob_ref)
    ev_blob = {
        "id": 102,
        "type": "message.appended",
        "session_id": "sess_blob",
        "actor": "cli:sess_blob",
        "payload_json": json.dumps({"role": "assistant", "content_blob_ref": "sha256:abc12345def"}),
    }
    raw_blob = format_sse_event(ev_blob)
    data_blob = json.loads([l for l in raw_blob.split("\n") if l.startswith("data: ")][0][6:])
    assert data_blob["is_redacted"] is False
    assert data_blob["has_blob"] is True
