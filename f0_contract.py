"""F0-Contract Specification & Constants for Avunel Serve & Cockpit.

Frozen per The Leader's approval:
- Port: 31032
- Host: 127.0.0.1
- Token authentication: Bearer header or ?token= query param
- Origin validation: loopback only
- SSE cursor: id > last_id
- Heartbeat: 15s interval
"""

from __future__ import annotations

import hmac
import json
import logging
import re
from typing import Any, Dict, Optional, Set, Tuple
from urllib.parse import urlparse

logger = logging.getLogger("avunel.f0_contract")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 31032
MIN_PORT = 1024
MAX_PORT = 65535

HEARTBEAT_INTERVAL_SECONDS = 15.0
POLL_INTERVAL_SECONDS = 0.25
MAX_SSE_BATCH_SIZE = 100

DEFAULT_ALLOWED_ORIGINS: Set[str] = {
    f"http://127.0.0.1:{DEFAULT_PORT}",
    f"http://localhost:{DEFAULT_PORT}",
}

EVENT_TYPE_SESSION_CREATED = "session.created"
EVENT_TYPE_SESSION_REWOUND = "session.rewound"
EVENT_TYPE_SESSION_TITLE_UPDATED = "session.title_updated"
EVENT_TYPE_MESSAGE_APPENDED = "message.appended"
EVENT_TYPE_BLOB_STORED = "blob.stored"

HEADER_AUTH_PREFIX = "Bearer "


def validate_port(port: Any) -> int:
    """Validate port number is within the allowed range (1024..65535)."""
    try:
        p = int(port)
    except (ValueError, TypeError):
        raise ValueError(f"Port must be an integer, got: {port!r}")

    if p < MIN_PORT or p > MAX_PORT:
        raise ValueError(f"Port {p} is out of valid user range ({MIN_PORT}..{MAX_PORT})")
    return p


def validate_host(host: str) -> str:
    """Validate that the server only binds to loopback."""
    host = host.strip()
    if host not in {"127.0.0.1", "localhost"}:
        raise ValueError(
            f"Security refusal: Avunel serve only binds to loopback (127.0.0.1). Attempted: {host}"
        )
    return host


def verify_token(provided_token: Optional[str], expected_token: str) -> bool:
    """Constant-time token comparison to prevent timing attacks."""
    if not provided_token or not expected_token:
        return False
    return hmac.compare_digest(provided_token.encode("utf-8"), expected_token.encode("utf-8"))


def is_origin_allowed(
    origin_header: Optional[str],
    port: int = DEFAULT_PORT,
    additional_allowed: Optional[Set[str]] = None,
) -> bool:
    """Validate Origin header against loopback origins.

    If Origin is None or empty (non-browser client like curl/TUI), return True.
    If Origin is present, ensure hostname is 127.0.0.1 or localhost on the valid port.
    """
    if not origin_header:
        return True

    parsed = urlparse(origin_header)
    allowed = {
        f"http://127.0.0.1:{port}",
        f"http://localhost:{port}",
    }
    if additional_allowed:
        allowed.update(additional_allowed)

    # Reconstruct normalized origin scheme://netloc
    norm_origin = f"{parsed.scheme}://{parsed.netloc}"
    return norm_origin in allowed


_REDACTION_PATTERN = re.compile(
    r"(?:sk-[a-zA-Z0-9_\-]*|[a-zA-Z0-9_\-]{3,8})\.\.\.[a-zA-Z0-9_\-]{3,6}|«redacted:|\[REDACTED:"
)


def is_event_redacted(event_dict: Dict[str, Any]) -> bool:
    """Determine whether an event contains redacted secret data."""
    if "is_redacted" in event_dict:
        return bool(event_dict["is_redacted"])

    payload_raw = event_dict.get("payload_json")
    if not payload_raw and "payload" in event_dict:
        try:
            payload_raw = json.dumps(event_dict["payload"])
        except Exception:
            payload_raw = None

    if payload_raw and isinstance(payload_raw, str):
        if _REDACTION_PATTERN.search(payload_raw):
            return True

    return False


def event_has_blob(event_dict: Dict[str, Any]) -> bool:
    """Determine whether an event references offloaded CAS blob content."""
    if "has_blob" in event_dict:
        return bool(event_dict["has_blob"])

    if event_dict.get("type") == EVENT_TYPE_BLOB_STORED:
        return True

    api_ref = event_dict.get("api_content_ref")
    if api_ref and (api_ref.startswith("sha256:") or api_ref.startswith("blob:")):
        return True

    payload = event_dict.get("payload")
    if not payload:
        payload_str = event_dict.get("payload_json")
        if payload_str and isinstance(payload_str, str):
            try:
                payload = json.loads(payload_str)
            except Exception:
                payload = {}
    if isinstance(payload, dict):
        if payload.get("content_blob_ref") or payload.get("tool_calls_blob_ref"):
            return True

    return False


def format_sse_event(event_dict: Dict[str, Any]) -> str:
    """Format an event dictionary as a standard W3C SSE message.

    Per Contract §2, the serialized JSON data includes 'is_redacted' and 'has_blob'.
    """
    event_id = event_dict.get("id")
    event_type = event_dict.get("type", "message")

    ev_copy = dict(event_dict)
    ev_copy["is_redacted"] = is_event_redacted(event_dict)
    ev_copy["has_blob"] = event_has_blob(event_dict)

    data_str = json.dumps(ev_copy, ensure_ascii=False)
    return f"id: {event_id}\nevent: {event_type}\ndata: {data_str}\n\n"


def format_sse_heartbeat(timestamp: float) -> str:
    """Format an SSE comment heartbeat."""
    return f": heartbeat {timestamp:.3f}\n\n"
