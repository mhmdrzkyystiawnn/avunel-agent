"""F0-09: Full-Text Search (FTS5) for append-only events log (D-14).

Provides:
- FTS5 virtual table ``events_fts`` over ``events_effective`` view.
- Content synchronization via ``AFTER INSERT`` trigger on ``events`` and redaction hook.
- Search interface ``search_events()`` with ranking and session/type filters.
- Blob message snippet search: for messages with offloaded content, FTS searches
  through ``<field>_snippet`` (up to 2 KiB), satisfying narrowed G0b.
- Corruption detection and recovery (ported from hermes_state_fts.py).
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes_state.events_fts")

FTS_TABLE_NAME = "events_fts"

CREATE_EVENTS_FTS_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
    payload_json,
    type,
    session_id,
    content='events_effective',
    content_rowid='id'
);
"""

CREATE_EVENTS_FTS_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS events_fts_ai AFTER INSERT ON events BEGIN
    INSERT INTO events_fts(rowid, payload_json, type, session_id)
    VALUES (new.id, new.payload_json, new.type, new.session_id);
END;
"""


def ensure_events_fts(conn: sqlite3.Connection) -> None:
    """Ensure events_fts virtual table and after-insert trigger exist on events.db."""
    conn.execute(CREATE_EVENTS_FTS_SQL)
    conn.execute(CREATE_EVENTS_FTS_TRIGGER_SQL)


def rebuild_events_fts(conn: sqlite3.Connection) -> None:
    """Rebuild the events_fts index from events_effective."""
    conn.execute(f"INSERT INTO {FTS_TABLE_NAME}({FTS_TABLE_NAME}) VALUES('rebuild')")


def is_events_fts_corruption_error(exc: Exception) -> bool:
    """Determine whether an exception indicates an FTS5-scoped index corruption."""
    msg = str(exc).lower()
    corruption_markers = (
        "malformed",
        "database disk image is malformed",
        "fts5: error",
        "corrupt",
        "constraint failed",
    )
    return any(marker in msg for marker in corruption_markers)


def repair_events_fts_if_corrupt(conn: sqlite3.Connection, exc: Exception) -> bool:
    """Attempt to repair events_fts if the given exception is an FTS corruption error.

    Returns True if repair was attempted and succeeded, False otherwise.
    """
    if not is_events_fts_corruption_error(exc):
        return False

    logger.warning("FTS corruption detected (%s); attempting rebuild...", exc)
    try:
        rebuild_events_fts(conn)
        logger.info("FTS rebuild succeeded.")
        return True
    except Exception as rebuild_exc:
        logger.warning("FTS rebuild failed (%s); dropping and re-creating events_fts...", rebuild_exc)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {FTS_TABLE_NAME}")
            ensure_events_fts(conn)
            rebuild_events_fts(conn)
            logger.info("FTS re-creation and rebuild succeeded.")
            return True
        except Exception as recreate_exc:
            logger.error("Failed to recover events_fts: %s", recreate_exc)
            return False


def search_events(
    conn: sqlite3.Connection,
    query: str,
    *,
    session_id: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Search events using FTS5 match query over payload_json, type, and session_id.

    Returns matching events ordered by FTS rank.
    For messages offloaded to blob store, search operates over <field>_snippet (up to 2 KiB).
    """
    if not query or not query.strip():
        return []

    sql = """
        SELECT e.id, e.type, e.ts, e.session_id, e.payload_json, e.api_content_ref
        FROM events_fts f
        JOIN events_effective e ON f.rowid = e.id
        WHERE events_fts MATCH ?
    """
    params: List[Any] = [query]

    if session_id:
        sql += " AND e.session_id = ?"
        params.append(session_id)

    if event_type:
        sql += " AND e.type = ?"
        params.append(event_type)

    sql += " ORDER BY rank LIMIT ?"
    params.append(limit)

    try:
        rows = conn.execute(sql, params).fetchall()
    except Exception as exc:
        if is_events_fts_corruption_error(exc):
            if repair_events_fts_if_corrupt(conn, exc):
                rows = conn.execute(sql, params).fetchall()
            else:
                raise
        else:
            raise

    results = []
    for r in rows:
        results.append({
            "id": r[0],
            "type": r[1],
            "ts": r[2],
            "session_id": r[3],
            "payload_json": r[4],
            "api_content_ref": r[5],
        })
    return results
