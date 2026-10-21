"""Append-only event log (F0-02, F0-03): the durable record that ``state.db`` is projected from.

Decisions this file encodes (see planning/avunel-planning.md §4.2):

* **K7** — 13 columns: ``id, parent_id, ts, type, v, session_id, branch_id, actor,
  correlation_id, payload_json, api_content_ref, snapshot_ref, created_at``.
* **D-04** — ``branch_id`` references a separate ``branches`` table, so a foreign key is
  valid from the first day.
* **D-06** — ``payload_json`` is capped at 64 KiB, measured in **bytes**
  (``length(CAST(... AS BLOB))``), not characters. Larger output belongs in the blob store.
* **D-07** — ``api_content_ref`` is required for ``message.*`` and ``generation.*`` events and
  NULL for everything else. Enforced by a boolean ``CHECK`` (SQLite has no ``CHECK ... WHERE``).
* **D-08 / D-09** — ``created_at`` is ``DEFAULT (datetime('now'))`` (no trigger: an
  ``AFTER INSERT`` trigger conflicts with the append-only guard, and ``BEFORE INSERT`` cannot
  see "was this set explicitly" because ``NEW`` already carries the default). Derived columns
  (``display_order``, ``display_identity``, ``message_uid``) are **not** stored here.
* **D-14** — ``redact(event_id, reason, ...)`` writes to a separate ``redactions`` table
  (overlaid at projection time via the ``events_effective`` view), sanitizes ``reason``, and
  syncs the FTS index when ``events_fts`` is present, keeping the append-only event log unmutated.

Nothing in this module writes to ``state.db`` — that is the dual-writer's job (F0-06).
"""

from __future__ import annotations

from contextlib import contextmanager
import json
import logging
from pathlib import Path
import sqlite3
import threading
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional

from agent.redact import redact_sensitive_text
from hermes_constants import get_hermes_home, mkdir_under_hermes_home

logger = logging.getLogger(__name__)

__all__ = [
    "EVENTS_SCHEMA_VERSION",
    "PAYLOAD_JSON_MAX_BYTES",
    "EventStore",
    "acquire",
    "events_db_path",
    "open_events",
    "redact_payload",
]

_REGISTRY: dict[Path, EventStore] = {}
_REGISTRY_LOCK = threading.Lock()


def acquire(db_path: Optional[Path | str] = None) -> EventStore:
    """Return the shared EventStore for *db_path* (canonicalized, thread-safe)."""
    raw_path = Path(db_path) if db_path is not None else events_db_path()
    try:
        path = raw_path.expanduser().resolve()
    except OSError:
        path = raw_path.expanduser()

    with _REGISTRY_LOCK:
        store = _REGISTRY.get(path)
        if store is None:
            store = EventStore(path)
            _REGISTRY[path] = store
        return store


def _clear_registry() -> None:
    """Clear the global EventStore registry (used by tests)."""
    with _REGISTRY_LOCK:
        _REGISTRY.clear()

#: Bumped when the table shape below changes; stamped into ``events.meta`` on open.
EVENTS_SCHEMA_VERSION = 1

#: D-06: payload cap in bytes (64 KiB).
PAYLOAD_JSON_MAX_BYTES = 64 * 1024

_D07_CHECK = (
    "((type NOT LIKE 'message.%') AND (type NOT LIKE 'generation.%')) "
    "OR (api_content_ref IS NOT NULL)"
)


def events_db_path() -> Path:
    """Where the event log lives: ``<hermes home>/events.db`` (K4)."""
    return Path(get_hermes_home()) / "events.db"


_CREATE_BRANCHES = """
CREATE TABLE IF NOT EXISTS branches (
    id          TEXT PRIMARY KEY,
    label       TEXT,
    parent_id   TEXT,
    FOREIGN KEY (parent_id) REFERENCES branches(id)
)
"""

_CREATE_EVENTS = f"""
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY,
    parent_id       INTEGER,
    ts              INTEGER NOT NULL,
    type            TEXT NOT NULL,
    v               INTEGER NOT NULL DEFAULT 1,
    session_id      TEXT,
    branch_id       TEXT,
    actor           TEXT,
    correlation_id  TEXT,
    payload_json    TEXT NOT NULL CHECK (length(CAST(payload_json AS BLOB)) <= {PAYLOAD_JSON_MAX_BYTES}),
    api_content_ref TEXT,
    snapshot_ref    TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    CHECK ({_D07_CHECK}),
    FOREIGN KEY (branch_id) REFERENCES branches(id)
)
"""

# Append-only: an event log cannot be amended, only appended.
_CREATE_NO_UPDATE = """
CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only: UPDATE blocked');
END
"""

_CREATE_NO_DELETE = """
CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events
BEGIN
    SELECT RAISE(ABORT, 'events is append-only: DELETE blocked');
END
"""

# D-14: Redactions overlay table — stores redacted payloads without modifying append-only events.
_CREATE_REDACTIONS = """
CREATE TABLE IF NOT EXISTS redactions (
    event_id              INTEGER PRIMARY KEY,
    reason                TEXT,
    redacted_payload_json TEXT NOT NULL,
    created_at            TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (event_id) REFERENCES events(id)
)
"""

# Guardrail 2: Single door for overlay queries (events_effective view).
_CREATE_EVENTS_EFFECTIVE = """
CREATE VIEW IF NOT EXISTS events_effective AS
SELECT
    e.id,
    e.parent_id,
    e.ts,
    e.type,
    e.v,
    e.session_id,
    e.branch_id,
    e.actor,
    e.correlation_id,
    COALESCE(r.redacted_payload_json, e.payload_json) AS payload_json,
    e.api_content_ref,
    e.snapshot_ref,
    e.created_at
FROM events e
LEFT JOIN redactions r ON e.id = r.event_id
"""


def redact_payload(payload: Any) -> Any:
    """Recursively redact secrets from payload objects before writing to events.db."""
    if isinstance(payload, str):
        stripped = payload.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (stripped.startswith("[") and stripped.endswith("]")):
            try:
                nested = json.loads(payload)
                return json.dumps(redact_payload(nested), ensure_ascii=False)
            except Exception:
                pass
        return redact_sensitive_text(payload, force=True)
    elif isinstance(payload, Mapping):
        return {k: redact_payload(v) for k, v in payload.items()}
    elif isinstance(payload, (list, tuple)):
        res = [redact_payload(item) for item in payload]
        return type(payload)(res)
    elif isinstance(payload, set):
        return {redact_payload(item) for item in payload}
    return payload


class EventStore:
    """The single writer for ``events.db``.

    One connection per process (the module keeps the first one created in this process), opened
    with ``WAL``, an explicit ``BEGIN IMMEDIATE`` and a 5s ``busy_timeout`` (F0-05).
    """

    def __init__(self, db_path: Optional[Path | str] = None) -> None:
        self._db_path = Path(db_path) if db_path is not None else events_db_path()
        self._local = threading.local()
        # (event_type, from_v) -> (to_v, fn)
        self._upgraders: dict[tuple[str, int], tuple[int, Callable[[dict[str, Any]], dict[str, Any]]]] = {}
        self._initialize()

    def register_payload_upgrader(
        self,
        event_type: str,
        from_v: int,
        to_v: int,
        fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        """Register a payload upgrade step for an event type (or '*' for all types)."""
        if to_v <= from_v:
            raise ValueError(f"to_v ({to_v}) must be greater than from_v ({from_v})")
        self._upgraders[(event_type, from_v)] = (to_v, fn)

    def _upgrade_payload(
        self, event_type: str, v: int, payload: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        curr_v = v
        curr_payload = dict(payload)
        visited: set[tuple[str, int]] = set()
        while True:
            key = (event_type, curr_v)
            if key not in self._upgraders:
                key = ("*", curr_v)
            if key not in self._upgraders:
                break
            if key in visited:
                raise RuntimeError(f"Cycle detected in payload upgraders at {key}")
            visited.add(key)
            next_v, fn = self._upgraders[key]
            curr_payload = fn(curr_payload)
            curr_v = next_v
        return curr_v, curr_payload

    def _row_to_event_dict(self, row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
        if row is None:
            return None
        d = dict(row)
        curr_v = d.get("v", 1)
        event_type = d.get("type", "")
        try:
            payload = json.loads(d.get("payload_json", "{}"))
        except Exception:
            payload = {}

        final_v, upgraded_payload = self._upgrade_payload(event_type, curr_v, payload)
        d["v"] = final_v
        d["payload"] = upgraded_payload
        d["payload_json"] = json.dumps(upgraded_payload, ensure_ascii=False)
        return d

    # -- connection ---------------------------------------------------------

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), isolation_level=None, timeout=5.0)
        conn.row_factory = sqlite3.Row
        # F0-05: WAL + busy timeout + synchronous=FULL + foreign_keys=ON
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=5000")
        # D-04: foreign keys are OFF by default in SQLite; the events->branches FK is only
        # enforced when this is on.
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._connect()
        return conn

    def close(self) -> None:
        """Close connection for the current thread."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None
        self._local.depth = 0

    def _initialize(self) -> None:
        with self.acquire() as conn:
            conn.execute(_CREATE_BRANCHES)
            conn.execute(_CREATE_EVENTS)
            conn.execute(_CREATE_NO_UPDATE)
            conn.execute(_CREATE_NO_DELETE)
            conn.execute(_CREATE_REDACTIONS)
            conn.execute(_CREATE_EVENTS_EFFECTIVE)
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='meta'"
            ).fetchone()
            if row is None:
                conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
            version = conn.execute(
                "SELECT value FROM meta WHERE key='schema_version'"
            ).fetchone()
            if version is None:
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                    (str(EVENTS_SCHEMA_VERSION),),
                )

    @contextmanager
    def acquire(self) -> Iterator[sqlite3.Connection]:
        """Acquire a connection within a write transaction (BEGIN IMMEDIATE / SAVEPOINT).

        Re-entrant on the same thread:
        - Top-level (depth == 0): executes BEGIN IMMEDIATE, commits on clean exit,
          rolls back on exception.
        - Nested (depth > 0): uses SAVEPOINT sp_<depth>. If inner block raises an exception
          and is caught by caller, only inner changes are rolled back without corrupting
          outer transaction.
        """
        conn = self._conn()
        depth = getattr(self._local, "depth", 0)
        if depth == 0:
            conn.execute("BEGIN IMMEDIATE")
            self._local.depth = 1
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            finally:
                self._local.depth = 0
        else:
            sp_name = f"sp_{depth}"
            conn.execute(f"SAVEPOINT {sp_name}")
            self._local.depth = depth + 1
            try:
                yield conn
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
            except BaseException:
                conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                conn.execute(f"RELEASE SAVEPOINT {sp_name}")
                raise
            finally:
                self._local.depth = depth

    # Backwards-compatible alias for existing callers
    _write = acquire

    def commit(self) -> None:
        conn = self._conn()
        if conn.in_transaction:
            conn.execute("COMMIT")
        self._local.depth = 0

    def rollback(self) -> None:
        conn = self._conn()
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        self._local.depth = 0

    # -- writing ------------------------------------------------------------

    def emit(
        self,
        type: str,
        *,
        ts: int,
        v: int = 1,
        payload: Optional[Mapping[str, Any]] = None,
        session_id: Optional[str] = None,
        branch_id: Optional[str] = None,
        actor: Optional[str] = None,
        correlation_id: Optional[str] = None,
        api_content_ref: Optional[str] = None,
        snapshot_ref: Optional[str] = None,
        parent_id: Optional[int] = None,
        redact: bool = True,
    ) -> int:
        """Append one event and return its ``id`` (the global ordering key).

        Rules enforced (D-05, D-08, D-09, F0-04):
        1. ``created_at`` is deliberately not a parameter: D-08 makes it the database's business.
        2. ``payload`` must be a dict (JSON object) per Point 3.
        3. ``actor`` must follow 'domain:id' with non-empty parts per D-05 when provided.
        4. ``message_uid`` is required for ``message.*`` events per D-09.
        5. ``payload`` is pre-redacted before insertion unless ``redact=False``.
        """
        if payload is None:
            raw_payload: dict[str, Any] = {}
        elif isinstance(payload, Mapping):
            raw_payload = dict(payload)
        else:
            raise TypeError(f"payload must be a dict (JSON object), got {payload.__class__.__name__}")

        if not isinstance(v, int) or isinstance(v, bool) or v < 1:
            raise ValueError(f"v must be a positive integer, got {v!r}")

        if not isinstance(ts, int) or isinstance(ts, bool):
            raise TypeError(f"ts must be an int, got {ts.__class__.__name__}")

        if actor is not None:
            if not isinstance(actor, str):
                raise TypeError(f"actor must be a str, got {actor.__class__.__name__}")
            parts = actor.split(":", 1)
            if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
                raise ValueError(
                    f"Invalid actor format: {actor!r}. Must follow 'domain:id' with non-empty parts "
                    f"(e.g. 'cli:s1', 'tui:s1', 'desktop:s1', 'cron:j1', 'agent:sub1') per D-05"
                )

        if type.startswith("message."):
            uid = raw_payload.get("message_uid")
            if not isinstance(uid, str) or not uid.strip():
                raise ValueError(
                    f"Event of type {type!r} requires a non-empty string 'message_uid' in payload (D-09)"
                )

        clean_payload = redact_payload(raw_payload) if redact else raw_payload
        payload_json = json.dumps(clean_payload, ensure_ascii=False)
        with self.acquire() as conn:
            cursor = conn.execute(
                """
                INSERT INTO events
                    (parent_id, ts, type, v, session_id, branch_id, actor, correlation_id,
                     payload_json, api_content_ref, snapshot_ref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    parent_id, ts, type, v, session_id, branch_id, actor, correlation_id,
                    payload_json, api_content_ref, snapshot_ref,
                ),
            )
            assert cursor.lastrowid is not None, "INSERT did not produce a rowid"
            return int(cursor.lastrowid)

    def redact(
        self,
        event_id: int,
        reason: str,
        *,
        replacement_payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Redact a stored event on the append-only log via the redactions overlay (D-14).

        1. Sanitizes ``reason`` with ``redact_sensitive_text(reason, force=True)`` (Guardrail 1).
        2. Runs within an atomic ``acquire()`` transaction.
        3. Computes the clean replacement payload (from ``replacement_payload`` or auto-redacted).
        4. If ``events_fts`` is present, purges old index entries while ``events_effective`` still
           points to old content.
        5. Records into ``redactions`` table without updating ``events``.
        6. Re-indexes clean content into ``events_fts`` (D-14 / F0-09 contract).
        """
        clean_reason = redact_sensitive_text(reason, force=True) if reason else ""

        with self.acquire() as conn:
            row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
            if row is None:
                raise KeyError(f"Event with id={event_id} does not exist")

            if replacement_payload is not None:
                clean_payload = redact_payload(replacement_payload)
            else:
                try:
                    curr_payload = json.loads(row["payload_json"])
                except Exception:
                    curr_payload = {}
                clean_payload = redact_payload(curr_payload)

            redacted_payload_json = json.dumps(clean_payload, ensure_ascii=False)

            # Step A: delete from FTS while events_effective still holds the old content
            has_fts = self._fts_delete(conn, event_id)

            # Step B: insert into redactions (events_effective now yields clean payload)
            conn.execute(
                """
                INSERT OR REPLACE INTO redactions (event_id, reason, redacted_payload_json)
                VALUES (?, ?, ?)
                """,
                (event_id, clean_reason, redacted_payload_json),
            )

            # Step C: insert clean content into FTS
            if has_fts:
                self._fts_insert(
                    conn, event_id, row["type"], row["session_id"], redacted_payload_json
                )

    def _fts_delete(self, conn: sqlite3.Connection, event_id: int) -> bool:
        """Purge old index entries from events_fts before redactions table is updated."""
        fts = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events_fts'"
        ).fetchone()
        if fts is None:
            return False
        try:
            conn.execute("DELETE FROM events_fts WHERE rowid = ?", (event_id,))
            return True
        except Exception as e:
            logger.warning("FTS delete for event %d failed: %s", event_id, e)
            return False

    def _fts_insert(
        self,
        conn: sqlite3.Connection,
        event_id: int,
        event_type: str,
        session_id: Optional[str],
        clean_payload_json: str,
    ) -> None:
        """Re-index clean content into events_fts after redactions table is updated."""
        try:
            conn.execute(
                "INSERT INTO events_fts (rowid, payload_json, type, session_id) VALUES (?, ?, ?, ?)",
                (event_id, clean_payload_json, event_type, session_id or ""),
            )
        except Exception as e:
            logger.warning("FTS re-insert for event %d failed: %s", event_id, e)

    # -- reading ------------------------------------------------------------

    def get_event(self, event_id: int) -> Optional[dict[str, Any]]:
        """Fetch event with redaction overlay and payload upgrader applied (Guardrails 2 & F0-04)."""
        row = self._conn().execute(
            "SELECT * FROM events_effective WHERE id = ?", (event_id,)
        ).fetchone()
        return self._row_to_event_dict(row)

    def get_raw_event(self, event_id: int) -> Optional[dict[str, Any]]:
        """Fetch unredacted event directly from events table with payload upgrader applied."""
        row = self._conn().execute(
            "SELECT * FROM events WHERE id = ?", (event_id,)
        ).fetchone()
        return self._row_to_event_dict(row)

    def events_after(self, last_id: int, *, limit: int = 1000) -> list[dict[str, Any]]:
        """Fetch events after last_id ordered by id ASC with redactions and upgrades applied."""
        rows = self._conn().execute(
            "SELECT * FROM events_effective WHERE id > ? ORDER BY id ASC LIMIT ?",
            (last_id, limit),
        ).fetchall()
        return [self._row_to_event_dict(r) for r in rows if r is not None]

    def all_events(self) -> Iterable[dict[str, Any]]:
        """Iterate every event in id ASC order with redactions and upgrades applied."""
        conn = self._conn()
        cursor = conn.execute("SELECT * FROM events_effective ORDER BY id ASC")
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for r in rows:
                transformed = self._row_to_event_dict(r)
                if transformed is not None:
                    yield transformed

    def session_events(self, session_id: str) -> list[dict[str, Any]]:
        """Fetch all events for a given session_id ordered by id ASC."""
        rows = self._conn().execute(
            "SELECT * FROM events_effective WHERE session_id = ? ORDER BY id ASC",
            (session_id,),
        ).fetchall()
        return [self._row_to_event_dict(r) for r in rows if r is not None]

    def count(self) -> int:
        return int(self._conn().execute("SELECT COUNT(*) FROM events").fetchone()[0])


def open_events(db_path: Optional[Path | str] = None) -> EventStore:
    """Open (creating if necessary) the event log at ``db_path`` or the default location."""
    return EventStore(db_path)
