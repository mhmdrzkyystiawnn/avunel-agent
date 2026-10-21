"""F0-10: State.db to Events.db Importer (D-12).

Provides:
- Idempotent migration of sessions and messages from legacy/existing state.db to events.db.
- Transparent CAS blob offload for payloads > 60 KiB.
- Preservation of message_uid in payload_json (D-09).
- Exact idempotency (run 1x, 2x, 3x produces identical event log without duplicate events).
- Support for backlog sessions (core-talos, faktura-asli, hermes-dev).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Union

from hermes_state_events import EventStore
from blob_store import BlobStore, offload_payload_if_needed

logger = logging.getLogger("hermes_state.importer")

def init_importer_tracking(conn: sqlite3.Connection) -> None:
    """Ensure import tracking tables exist in destination events.db."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions_imported (
            session_id TEXT PRIMARY KEY,
            event_id INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages_imported (
            source_message_id INTEGER PRIMARY KEY,
            session_id TEXT NOT NULL,
            event_id INTEGER NOT NULL
        )
        """
    )


def import_state_to_events(
    source_db_path: Union[Path, str],
    event_store: EventStore,
    *,
    blob_store: Optional[BlobStore] = None,
    session_ids: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    """Import sessions and messages from source state.db into event_store.

    Idempotent: can be run repeatedly without duplicating events.
    Returns stats dict: {"sessions_imported": N, "messages_imported": M, "skipped_messages": S}.
    """
    src_conn = sqlite3.connect(f"file:{Path(source_db_path).resolve()}?mode=ro", uri=True)
    src_conn.row_factory = sqlite3.Row

    with event_store.acquire() as dst_conn:
        init_importer_tracking(dst_conn)

    # 1. Determine sessions to import
    if session_ids:
        qmarks = ",".join(["?"] * len(session_ids))
        s_rows = src_conn.execute(
            f"SELECT * FROM sessions WHERE id IN ({qmarks}) ORDER BY id ASC",
            list(session_ids),
        ).fetchall()
    else:
        s_rows = src_conn.execute("SELECT * FROM sessions ORDER BY id ASC").fetchall()

    sessions_imported = 0
    messages_imported = 0
    skipped_messages = 0

    for s in s_rows:
        sid = s["id"]
        # Check if session.created already emitted
        with event_store.acquire() as dst_conn:
            already = dst_conn.execute(
                "SELECT event_id FROM sessions_imported WHERE session_id = ?", (sid,)
            ).fetchone()

        if already is None:
            session_payload = {
                "source": s["source"] or "cli",
                "model": s["model"],
                "model_config": s["model_config"],
                "cwd": s["cwd"],
                "profile_name": s["profile_name"],
                "title": s["title"],
                "parent_session_id": s["parent_session_id"],
            }
            # ts in events.db is integer timestamp
            raw_ts = s["started_at"] if "started_at" in s.keys() else (s["created_at"] if "created_at" in s.keys() else 1)
            ts_val = int(raw_ts) if raw_ts else 1
            eid = event_store.emit(
                "session.created",
                ts=ts_val,
                session_id=sid,
                payload=session_payload,
            )
            with event_store.acquire() as dst_conn:
                dst_conn.execute(
                    "INSERT OR IGNORE INTO sessions_imported (session_id, event_id) VALUES (?, ?)",
                    (sid, eid),
                )
            sessions_imported += 1

        # 2. Import messages for this session in id ASC order
        m_rows = src_conn.execute(
            "SELECT * FROM messages WHERE session_id = ? ORDER BY id ASC", (sid,)
        ).fetchall()

        for m in m_rows:
            source_msg_id = m["id"]
            with event_store.acquire() as dst_conn:
                m_already = dst_conn.execute(
                    "SELECT event_id FROM messages_imported WHERE source_message_id = ?",
                    (source_msg_id,),
                ).fetchone()

            if m_already is not None:
                skipped_messages += 1
                continue

            # Ensure message_uid is non-empty string per D-09
            m_uid = m["message_uid"] or f"gen_{source_msg_id}"

            payload: Dict[str, Any] = {
                "message_uid": m_uid,
                "role": m["role"],
                "content": m["content"] or "",
                "tool_name": m["tool_name"],
                "tool_calls": m["tool_calls"],
                "tool_call_id": m["tool_call_id"],
                "tool_call_uids": m["tool_call_uids"],
                "tool_call_uid": m["tool_call_uid"],
                "token_count": m["token_count"],
                "finish_reason": m["finish_reason"],
                "reasoning": m["reasoning"],
                "reasoning_content": m["reasoning_content"],
                "reasoning_details": m["reasoning_details"],
                "codex_reasoning_items": m["codex_reasoning_items"],
                "codex_message_items": m["codex_message_items"],
                "platform_message_id": m["platform_message_id"],
                "observed": bool(m["observed"]),
                "effect_disposition": m["effect_disposition"],
                "display_kind": m["display_kind"],
                "display_metadata": m["display_metadata"],
                "_compressed_summary": bool(m["_compressed_summary"]),
            }
            # Clean None values to minimize footprint
            payload = {k: v for k, v in payload.items() if v is not None}

            # Transparent offload if > 60 KiB
            if blob_store is not None:
                payload = offload_payload_if_needed(payload, blob_store=blob_store, threshold_bytes=60000)

            ts_msg = int(m["timestamp"]) if m["timestamp"] else 1
            api_ref = f"msg:{m_uid}"

            msg_eid = event_store.emit(
                "message.appended",
                ts=ts_msg,
                session_id=sid,
                payload=payload,
                api_content_ref=api_ref,
            )

            with event_store.acquire() as dst_conn:
                dst_conn.execute(
                    "INSERT OR IGNORE INTO messages_imported (source_message_id, session_id, event_id) VALUES (?, ?, ?)",
                    (source_msg_id, sid, msg_eid),
                )
            messages_imported += 1

    src_conn.close()
    return {
        "sessions_imported": sessions_imported,
        "messages_imported": messages_imported,
        "skipped_messages": skipped_messages,
    }
