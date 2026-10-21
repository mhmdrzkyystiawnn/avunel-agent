"""Blob store for Avunel (F0-08 / D-10).

Content-addressable storage (CAS) for large tool outputs, byte-fidelity
api_content sidecars, and event payload overflow exceeding the D-06 64 KiB cap.

Features:
- Content-addressed via SHA-256 (format: "sha256:<64-hex-digest>").
- Two-level shard directory layout (root_dir / digest[:2] / digest[2:]) to
  prevent directory inode congestion.
- Atomic writes via same-filesystem temporary files and os.replace().
- Deduplicated: storing the same bytes multiple times is an idempotent no-op.
- Security-hardened: validates hash hex syntax, rejecting path traversal attempts.
- Supports streaming reads, line-range queries, and regex grep (F3-09 prep).
- Integrates with EventStore via api_content_ref (D-07).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, BinaryIO, Dict, Generator, List, Optional, Tuple, Union

from hermes_constants import get_hermes_home

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_REF_PREFIX = "sha256:"


def compute_sha256(data: bytes) -> str:
    """Compute SHA-256 hex digest of bytes."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


class BlobStore:
    """Content-addressable blob store on the local filesystem."""

    def __init__(self, root_dir: Union[Path, str]) -> None:
        self.root_dir = Path(root_dir).resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)
        try:
            self.root_dir.chmod(0o700)
        except OSError:
            pass

    def _parse_ref(self, ref: str) -> str:
        """Validate and return 64-char hex digest from a ref string."""
        if not isinstance(ref, str):
            raise TypeError(f"Blob ref must be a string, got {type(ref).__name__}")
        if not ref.startswith(_REF_PREFIX):
            raise ValueError(f"Invalid blob ref format: {ref!r}. Expected prefix '{_REF_PREFIX}'")
        digest = ref[len(_REF_PREFIX):].lower().strip()
        if not _SHA256_HEX_RE.match(digest):
            raise ValueError(f"Invalid SHA-256 digest in blob ref: {ref!r}")
        return digest

    def _path_for_digest(self, digest: str) -> Path:
        """Return the filesystem path for a valid SHA-256 digest."""
        shard_dir = self.root_dir / digest[:2]
        return shard_dir / digest[2:]

    def put(self, data: Union[bytes, str], encoding: str = "utf-8") -> str:
        """Write content to blob store and return its 'sha256:<digest>' reference.

        If identical content already exists, this is a deduplicated no-op.
        Filesystem permissions: root_dir and shard_dir are 0700; blob files are 0600.
        Writes are atomic via temporary file + fsync + os.replace + shard directory fsync.
        """
        if isinstance(data, str):
            raw_bytes = data.encode(encoding)
        elif isinstance(data, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(data)
        else:
            raise TypeError(f"data must be bytes or str, got {type(data).__name__}")

        digest = compute_sha256(raw_bytes)
        target_path = self._path_for_digest(digest)
        shard_dir = target_path.parent

        if target_path.exists():
            return f"{_REF_PREFIX}{digest}"

        if not shard_dir.exists():
            shard_dir.mkdir(parents=True, exist_ok=True)
            try:
                shard_dir.chmod(0o700)
            except OSError:
                pass

        # Atomic write via same-filesystem temporary file
        fd, tmp_file = tempfile.mkstemp(dir=shard_dir, prefix=".tmp_blob_")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(raw_bytes)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, target_path)

            # Fsync parent directory to ensure directory entry is flushed to durable media
            dir_fd = os.open(str(shard_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            if os.path.exists(tmp_file):
                try:
                    os.unlink(tmp_file)
                except OSError:
                    pass
            raise

        return f"{_REF_PREFIX}{digest}"

    def exists(self, ref: str) -> bool:
        """Check whether a blob reference exists in the store."""
        try:
            digest = self._parse_ref(ref)
        except (ValueError, TypeError):
            return False
        return self._path_for_digest(digest).exists()

    def size(self, ref: str) -> int:
        """Return the size in bytes of the referenced blob."""
        digest = self._parse_ref(ref)
        path = self._path_for_digest(digest)
        if not path.exists():
            raise KeyError(f"Blob not found: {ref}")
        return path.stat().st_size

    def get(self, ref: str) -> bytes:
        """Retrieve raw bytes of the referenced blob."""
        digest = self._parse_ref(ref)
        path = self._path_for_digest(digest)
        if not path.exists():
            raise KeyError(f"Blob not found: {ref}")
        return path.read_bytes()

    def get_text(self, ref: str, encoding: str = "utf-8") -> str:
        """Retrieve decoded text of the referenced blob."""
        return self.get(ref).decode(encoding, errors="replace")

    def open(self, ref: str) -> BinaryIO:
        """Open referenced blob for binary streaming read."""
        digest = self._parse_ref(ref)
        path = self._path_for_digest(digest)
        if not path.exists():
            raise KeyError(f"Blob not found: {ref}")
        return open(path, "rb")

    def read_range(
        self,
        ref: str,
        start_line: Optional[int] = None,
        end_line: Optional[int] = None,
        encoding: str = "utf-8",
    ) -> str:
        """Return lines between start_line and end_line (1-indexed inclusive)."""
        text = self.get_text(ref, encoding=encoding)
        lines = text.splitlines(keepends=True)
        total_lines = len(lines)

        s = 0 if start_line is None else max(0, start_line - 1)
        e = total_lines if end_line is None else min(total_lines, end_line)

        return "".join(lines[s:e])

    def grep(
        self,
        ref: str,
        pattern: str,
        encoding: str = "utf-8",
        max_matches: int = 1000,
    ) -> List[Tuple[int, str]]:
        """Search regex pattern line-by-line within the blob, returning (line_number, line_text)."""
        regex = re.compile(pattern)
        text = self.get_text(ref, encoding=encoding)
        matches: List[Tuple[int, str]] = []

        for line_no, line in enumerate(text.splitlines(), start=1):
            if regex.search(line):
                matches.append((line_no, line))
                if len(matches) >= max_matches:
                    break

        return matches

    def delete(self, ref: str, event_store: Optional[Any] = None) -> bool:
        """Delete blob file if it exists and is not referenced by any event.

        If event_store is provided, verifies that no stored event references
        this blob reference before unlinking.
        Returns True if deleted, False if not found or referenced by events.
        """
        digest = self._parse_ref(ref)
        path = self._path_for_digest(digest)
        if not path.exists():
            return False

        if event_store is not None:
            # Check if any event references this ref in api_content_ref or payload_json
            ref_str = f"{_REF_PREFIX}{digest}"
            with event_store.acquire() as conn:
                row = conn.execute(
                    "SELECT 1 FROM events WHERE api_content_ref = ? OR payload_json LIKE ? LIMIT 1",
                    (ref_str, f"%{ref_str}%"),
                ).fetchone()
                if row is not None:
                    return False  # Still referenced by events, refusal to delete

        path.unlink(missing_ok=True)

        # Cleanup empty shard directory if vacant
        try:
            shard_dir = path.parent
            if not any(shard_dir.iterdir()):
                shard_dir.rmdir()
        except OSError:
            pass

        return True


# ── Global / Singleton helpers ──────────────────────────────────────────

_GLOBAL_BLOB_STORE: Optional[BlobStore] = None


def open_blob_store(root_dir: Optional[Union[Path, str]] = None) -> BlobStore:
    """Create or open a BlobStore at root_dir (default: <HERMES_HOME>/blobs)."""
    if root_dir is None:
        root_dir = get_hermes_home() / "blobs"
    return BlobStore(root_dir)


def get_blob_store() -> BlobStore:
    """Return the global default BlobStore instance for the current process."""
    global _GLOBAL_BLOB_STORE
    if _GLOBAL_BLOB_STORE is None:
        _GLOBAL_BLOB_STORE = open_blob_store()
    return _GLOBAL_BLOB_STORE


def store_api_content(
    content: Optional[Union[str, bytes]],
    blob_store: Optional[BlobStore] = None,
) -> Optional[str]:
    """Store api_content string/bytes in blob store and return the api_content_ref.

    If content is None, returns None.
    """
    if content is None:
        return None
    store = blob_store or get_blob_store()
    return store.put(content)


def load_api_content(
    ref: Optional[str],
    blob_store: Optional[BlobStore] = None,
) -> Optional[str]:
    """Load api_content text from a blob reference, or return None if not a valid blob ref."""
    if not isinstance(ref, str) or not ref.startswith(_REF_PREFIX):
        return None
    store = blob_store or get_blob_store()
    try:
        return store.get_text(ref)
    except KeyError:
        return None


# ── Payload Offload Helpers (F0-08 / D-06) ──────────────────────────────

SNIPPET_MAX_BYTES: int = 2 * 1024  # 2,048 bytes (2 KiB)
PAYLOAD_OFFLOAD_THRESHOLD_BYTES: int = 60 * 1024  # 61,440 bytes (safe margin under 65,536 B cap)
OFFLOAD_CANDIDATE_FIELDS: Tuple[str, ...] = (
    "content",
    "tool_calls",
    "reasoning",
    "reasoning_content",
    "reasoning_details",
    "codex_reasoning_items",
    "codex_message_items",
    "display_metadata",
)


def offload_payload_if_needed(
    payload: Dict[str, Any],
    blob_store: Optional[BlobStore] = None,
    threshold_bytes: int = PAYLOAD_OFFLOAD_THRESHOLD_BYTES,
) -> Dict[str, Any]:
    """Offload large fields to BlobStore one by one if payload_json exceeds threshold.

    Payload carries <field>_blob_ref, <field>_size, and <field>_snippet (<= 2 KiB).
    Sensitive text is redacted before blob.put().
    Original string is preserved verbatim without loads/dumps re-encoding.
    """
    raw_json = json.dumps(payload, ensure_ascii=False)
    if len(raw_json.encode("utf-8")) <= threshold_bytes:
        return payload

    from agent.redact import redact_sensitive_text

    p = dict(payload)
    store = blob_store or get_blob_store()

    # Identify candidate fields present in payload
    candidates = []
    for field in OFFLOAD_CANDIDATE_FIELDS:
        val = p.get(field)
        if val is None or val == "" or val == [] or val == {}:
            continue
        if isinstance(val, str):
            val_str = val
        else:
            val_str = json.dumps(val, ensure_ascii=False)
        val_bytes = val_str.encode("utf-8")
        candidates.append((len(val_bytes), field, val_str))

    # Sort descending by size (largest field first)
    candidates.sort(key=lambda x: x[0], reverse=True)

    for field_size, field, val_str in candidates:
        # Redact sensitive text BEFORE blob.put() per spec
        redacted_text = redact_sensitive_text(val_str)
        redacted_bytes = redacted_text.encode("utf-8")

        blob_ref = store.put(redacted_bytes)
        size = len(redacted_bytes)
        snippet = redacted_bytes[:SNIPPET_MAX_BYTES].decode("utf-8", errors="replace")

        # Set payload fields
        p[field] = None
        p[f"{field}_blob_ref"] = blob_ref
        p[f"{field}_size"] = size
        p[f"{field}_snippet"] = snippet

        # Check if payload now fits under threshold
        current_len = len(json.dumps(p, ensure_ascii=False).encode("utf-8"))
        if current_len <= threshold_bytes:
            break

    return p


def restore_payload_fields(
    payload: Dict[str, Any],
    blob_store: Optional[BlobStore] = None,
) -> Dict[str, Any]:
    """Restore offloaded fields from BlobStore into payload dict.

    Returns payload with original strings restored verbatim without json.loads/dumps re-encoding.
    """
    p = dict(payload)
    store = blob_store or get_blob_store()

    for field in OFFLOAD_CANDIDATE_FIELDS:
        blob_ref = p.get(f"{field}_blob_ref")
        if blob_ref and (p.get(field) is None):
            try:
                p[field] = store.get_text(blob_ref)
            except KeyError:
                pass

    return p
