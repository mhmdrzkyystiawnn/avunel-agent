"""DualSink for Avunel (F0-06).

Dual-write coordinator implementing AVUNEL_STATE_MODE=dual:
Writes events to EventStore (events.db) as the primary append-only log,
and projects state to SessionDB (state.db) as the secondary read projection.

Parity Contract (F0-06):
state.db is identical to the projection replay of events.db, EXCEPT:
1. Portions redacted via EventStore pre-write / overlay redactions
   (events.db redacts secrets before write, so replaying from events.db
   produces redacted values [REDACTED:...], whereas state.db may hold
   unredacted raw strings).
2. api_content sidecar (waiting for F0-08 Blob store).

Open Items / Postponed (DITUNDA):
- Compression (publish_compression_child)
- /branch
These remain open items in planning/avunel-planning.md and are NOT implemented in F0-06.

Failure Handling:
- If event_store.emit fails, DualSink fails fast (raises immediately) and state.db is never touched.
- If event_store.emit succeeds but writing to state.db fails, the failure is fatal (exception propagates to caller).
Unprojected events remain in events.db and can be caught up via replay_pending().
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from agent.message_metadata import mint_uid
from agent.message_sanitization import coalesce_tool_call_id
from blob_store import (
    BlobStore,
    load_api_content,
    store_api_content,
    offload_payload_if_needed,
    restore_payload_fields,
)
from hermes_state import SessionDB
import hermes_state_events
from hermes_state_events import EventStore
from hermes_state_messages import _parse_tool_calls

logger = logging.getLogger("avunel.dual_sink")


class DualSink:
    """Dual-write coordinator between EventStore and SessionDB.

    Attributes:
        session_db: Underlying SessionDB instance (state.db).
        event_store: Underlying EventStore instance (events.db).
        mode: Operation mode ('legacy' or 'dual').
        last_projected_event_id: Highest event id successfully projected to state.db.
    """

    def __init__(
        self,
        session_db: SessionDB,
        event_store: Optional[EventStore] = None,
        blob_store: Optional[BlobStore] = None,
        mode: Optional[str] = None,
    ) -> None:
        self.session_db = session_db
        self.blob_store = blob_store
        resolved_mode = (mode or os.environ.get("AVUNEL_STATE_MODE", "legacy")).lower().strip()
        if resolved_mode not in ("legacy", "dual"):
            raise ValueError(f"Invalid state mode: {resolved_mode!r}. Expected 'legacy' or 'dual'.")
        self.mode = resolved_mode

        if event_store is not None:
            self.event_store: Optional[EventStore] = event_store
        elif self.mode == "dual":
            self.event_store = hermes_state_events.acquire()
        else:
            self.event_store = None

        self.last_projected_event_id: int = 0
        self._underlying_rewind_to_message = self.session_db.rewind_to_message

    def __getattr__(self, name: str) -> Any:
        """Delegate non-overridden attributes to the underlying session_db."""
        return getattr(self.session_db, name)

    def __enter__(self) -> DualSink:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close both session_db and event_store."""
        if hasattr(self.session_db, "close"):
            try:
                self.session_db.close()
            except Exception as exc:
                logger.warning("Error closing session_db: %s", exc)
        if self.event_store is not None and hasattr(self.event_store, "close"):
            try:
                self.event_store.close()
            except Exception as exc:
                logger.warning("Error closing event_store: %s", exc)

    # ── Session Operations ──────────────────────────────────────────────

    def create_session(self, session_id: str, source: str, **kwargs: Any) -> str:
        """Create (upsert) a session record in state.db, emitting session.created in dual mode."""
        if self.mode == "legacy":
            return self.session_db.create_session(session_id, source, **kwargs)

        assert self.event_store is not None, "event_store is required in dual mode"
        actor = kwargs.pop("actor", None)
        if not actor:
            actor = f"{source}:{session_id}" if ":" not in source else source
        correlation_id = kwargs.pop("correlation_id", None)

        payload = {
            "session_id": session_id,
            "source": source,
            "model": kwargs.get("model"),
            "model_config": kwargs.get("model_config"),
            "system_prompt": kwargs.get("system_prompt"),
            "user_id": kwargs.get("user_id"),
            "session_key": kwargs.get("session_key"),
            "chat_id": kwargs.get("chat_id"),
            "chat_type": kwargs.get("chat_type"),
            "thread_id": kwargs.get("thread_id"),
            "parent_session_id": kwargs.get("parent_session_id"),
            "cwd": kwargs.get("cwd"),
            "profile_name": kwargs.get("profile_name"),
            "git_repo_root": kwargs.get("git_repo_root"),
            "origin_json": kwargs.get("origin_json"),
            "display_name": kwargs.get("display_name"),
            "transport_profile": kwargs.get("transport_profile"),
            "started_at": kwargs.get("started_at"),
        }

        # 1. Primary write: emit to events.db (fail-fast if emit fails)
        event_id = self.event_store.emit(
            type="session.created",
            ts=int(time.time() * 1000),
            payload=payload,
            session_id=session_id,
            actor=actor,
            correlation_id=correlation_id,
        )

        # 2. Secondary write: project to state.db (fatal on failure)
        res = self.session_db.create_session(session_id, source, **kwargs)
        self.last_projected_event_id = max(self.last_projected_event_id, event_id)
        return res

    def ensure_session(
        self, session_id: str, source: str = "unknown", model: str = None, **kwargs: Any
    ) -> str:
        """Ensure a session row exists (upsert), emitting session.created in dual mode."""
        if self.mode == "legacy":
            return self.session_db.ensure_session(session_id, source=source, model=model, **kwargs)

        assert self.event_store is not None, "event_store is required in dual mode"
        actor = kwargs.pop("actor", None)
        if not actor:
            actor = f"{source}:{session_id}" if ":" not in source else source
        correlation_id = kwargs.pop("correlation_id", None)

        payload = {
            "session_id": session_id,
            "source": source,
            "model": model,
            "model_config": kwargs.get("model_config"),
            "system_prompt": kwargs.get("system_prompt"),
            "user_id": kwargs.get("user_id"),
            "session_key": kwargs.get("session_key"),
            "chat_id": kwargs.get("chat_id"),
            "chat_type": kwargs.get("chat_type"),
            "thread_id": kwargs.get("thread_id"),
            "parent_session_id": kwargs.get("parent_session_id"),
            "cwd": kwargs.get("cwd"),
            "profile_name": kwargs.get("profile_name"),
            "git_repo_root": kwargs.get("git_repo_root"),
            "origin_json": kwargs.get("origin_json"),
            "display_name": kwargs.get("display_name"),
            "transport_profile": kwargs.get("transport_profile"),
            "started_at": kwargs.get("started_at"),
        }

        event_id = self.event_store.emit(
            type="session.created",
            ts=int(time.time() * 1000),
            payload=payload,
            session_id=session_id,
            actor=actor,
            correlation_id=correlation_id,
        )

        res = self.session_db.ensure_session(session_id, source=source, model=model, **kwargs)
        self.last_projected_event_id = max(self.last_projected_event_id, event_id)
        return res

    def set_session_title(self, session_id: str, title: str, **kwargs: Any) -> bool:
        """Set session title in state.db, emitting session.title_updated in dual mode."""
        if self.mode == "legacy":
            return self.session_db.set_session_title(session_id, title)

        assert self.event_store is not None, "event_store is required in dual mode"
        actor = kwargs.pop("actor", None) or f"session:{session_id}"
        correlation_id = kwargs.pop("correlation_id", None)
        source = kwargs.pop("source", "user")

        payload = {
            "session_id": session_id,
            "title": title,
            "source": source,
        }

        event_id = self.event_store.emit(
            type="session.title_updated",
            ts=int(time.time() * 1000),
            payload=payload,
            session_id=session_id,
            actor=actor,
            correlation_id=correlation_id,
        )

        res = self.session_db.set_session_title(session_id, title)
        self.last_projected_event_id = max(self.last_projected_event_id, event_id)
        return res

    # ── Message Operations ──────────────────────────────────────────────

    def append_message(
        self,
        session_id: str,
        role: str,
        content: str = None,
        **kwargs: Any,
    ) -> Optional[int]:
        """Append one message; in dual mode emits message.appended outside _execute_write.

        Payload contains ONLY message data fields. Excludes transcript write guards:
        compression_lock_holder, turn_lease_holder, turn_lease_ttl_seconds.
        """
        if self.mode == "legacy":
            return self.session_db.append_message(session_id=session_id, role=role, content=content, **kwargs)

        assert self.event_store is not None, "event_store is required in dual mode"

        # Ensure message_uid is stamped so events.db and state.db share exact identity
        message_uid = kwargs.get("message_uid")
        if not message_uid or not isinstance(message_uid, str) or not message_uid.strip():
            message_uid = mint_uid()
            kwargs["message_uid"] = message_uid

        # Ensure tool_call_uids and tool_call_uid are stamped so events.db and state.db share exact identity
        tool_call_uids = kwargs.pop("tool_call_uids", None) or kwargs.pop("_tool_call_uids", None)
        if not tool_call_uids and role == "assistant" and kwargs.get("tool_calls"):
            parsed_tcs = _parse_tool_calls(kwargs.get("tool_calls"))
            if parsed_tcs:
                tool_call_uids = {}
                for tc in parsed_tcs:
                    cid = coalesce_tool_call_id(tc)
                    if cid and cid not in tool_call_uids:
                        tool_call_uids[cid] = mint_uid()

        tool_call_uid = kwargs.pop("tool_call_uid", None) or kwargs.pop("_tool_call_uid", None)

        actor = kwargs.pop("actor", None) or f"session:{session_id}"
        correlation_id = kwargs.pop("correlation_id", None)
        api_content = kwargs.get("api_content")
        api_content_ref = kwargs.pop("api_content_ref", None)
        if api_content_ref is None and api_content is not None:
            api_content_ref = store_api_content(api_content, blob_store=self.blob_store)
        if api_content_ref is None:
            api_content_ref = f"msg:{message_uid}"

        # Clean payload: ONLY message data (per User Instruction 4)
        payload = {
            "session_id": session_id,
            "role": role,
            "content": content,
            "message_uid": message_uid,
            "tool_name": kwargs.get("tool_name"),
            "tool_calls": kwargs.get("tool_calls"),
            "tool_call_id": kwargs.get("tool_call_id"),
            "tool_call_uids": tool_call_uids,
            "tool_call_uid": tool_call_uid,
            "token_count": kwargs.get("token_count"),
            "finish_reason": kwargs.get("finish_reason"),
            "reasoning": kwargs.get("reasoning"),
            "reasoning_content": kwargs.get("reasoning_content"),
            "reasoning_details": kwargs.get("reasoning_details"),
            "codex_reasoning_items": kwargs.get("codex_reasoning_items"),
            "codex_message_items": kwargs.get("codex_message_items"),
            "platform_message_id": kwargs.get("platform_message_id"),
            "observed": bool(kwargs.get("observed", False)),
            "effect_disposition": kwargs.get("effect_disposition"),
            "timestamp": kwargs.get("timestamp"),
            "display_kind": kwargs.get("display_kind"),
            "display_metadata": kwargs.get("display_metadata"),
            "_compressed_summary": bool(kwargs.get("_compressed_summary", False)),
        }

        # Offload large fields to blob store if payload_json exceeds threshold (D-06 64 KiB cap)
        payload = offload_payload_if_needed(payload, blob_store=self.blob_store)

        # 1. Primary write: emit event outside _execute_write
        event_id = self.event_store.emit(
            type="message.appended",
            ts=int(time.time() * 1000),
            payload=payload,
            session_id=session_id,
            actor=actor,
            correlation_id=correlation_id,
            api_content_ref=api_content_ref,
        )

        # 2. Secondary write: project to state.db (fatal on failure: exception propagates to caller)
        row_id = self.session_db.append_message(
            session_id=session_id,
            role=role,
            content=content,
            tool_call_uids=tool_call_uids,
            tool_call_uid=tool_call_uid,
            **kwargs,
        )
        self.last_projected_event_id = max(self.last_projected_event_id, event_id)
        return row_id

    # ── Undo / Rewind Operations ────────────────────────────────────────

    def rewind_to_message(
        self,
        session_id: str,
        target_message_id: int,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Soft-delete messages from target_message_id onwards; emits conversation.undone in dual mode."""
        if self.mode == "legacy":
            return self.session_db.rewind_to_message(session_id, target_message_id, **kwargs)

        assert self.event_store is not None, "event_store is required in dual mode"

        # Validate target in state.db and capture uids being undone before deactivation
        target_row = self.session_db._read_one(
            "SELECT role FROM messages WHERE id = ? AND session_id = ?",
            (target_message_id, session_id),
        )
        if target_row is None:
            raise ValueError(f"message {target_message_id} not found in session {session_id}")
        if target_row["role"] != "user":
            raise ValueError(
                f"rewind target must be a 'user' message (got role={target_row['role']!r}, id={target_message_id})"
            )

        rows = self.session_db._read_all(
            "SELECT message_uid FROM messages WHERE session_id = ? AND id >= ? AND active = 1 ORDER BY id ASC",
            (session_id, target_message_id),
        )
        undone_uids = [r["message_uid"] for r in rows if r["message_uid"]]

        actor = kwargs.pop("actor", None) or f"session:{session_id}"
        correlation_id = kwargs.pop("correlation_id", None)
        source = kwargs.pop("source", "user")
        target_ordinal = kwargs.pop("target_ordinal", None)

        payload = {
            "session_id": session_id,
            "target_message_id": target_message_id,
            "target_ordinal": target_ordinal,
            "count": len(undone_uids),
            "source": source,
            "undone_message_uids": undone_uids,
        }

        # 1. Primary write: emit conversation.undone
        event_id = self.event_store.emit(
            type="conversation.undone",
            ts=int(time.time() * 1000),
            payload=payload,
            session_id=session_id,
            actor=actor,
            correlation_id=correlation_id,
        )

        # 2. Secondary write: project to state.db (fatal on failure)
        outcome = self._underlying_rewind_to_message(session_id, target_message_id, **kwargs)
        self.last_projected_event_id = max(self.last_projected_event_id, event_id)
        return outcome

    def rewind_user_turn(
        self,
        session_id: str,
        user_ordinal: int,
        **kwargs: Any,
    ) -> Any:
        """Rewind active transcript to user_ordinal; in dual mode intercepts rewind_to_message to emit event."""
        if self.mode == "legacy":
            return self.session_db.rewind_user_turn(session_id, user_ordinal, **kwargs)

        orig_rewind = self.session_db.rewind_to_message

        def _intercepted_rewind(s_id: str, t_id: int, **r_kwargs: Any) -> Dict[str, Any]:
            return self.rewind_to_message(s_id, t_id, target_ordinal=user_ordinal, **r_kwargs)

        try:
            self.session_db.rewind_to_message = _intercepted_rewind  # type: ignore[assignment]
            return self.session_db.rewind_user_turn(session_id, user_ordinal, **kwargs)
        finally:
            self.session_db.rewind_to_message = orig_rewind  # type: ignore[assignment]

    # ── Projection & Replay (Idempotent per Decision 3) ─────────────────

    def project_event(
        self,
        event: Union[sqlite3.Row, Dict[str, Any]],
        session_db: Optional[SessionDB] = None,
    ) -> bool:
        """Project a single event into state.db idempotently using (session_id, message_uid).

        No filter on active for message lookup.
        """
        target_db = session_db or self.session_db
        event_dict = dict(event) if isinstance(event, sqlite3.Row) else event
        event_type = event_dict.get("type", "")
        session_id = event_dict.get("session_id")

        raw_payload = event_dict.get("payload")
        if raw_payload is None:
            payload_json_str = event_dict.get("payload_json", "{}")
            try:
                raw_payload = json.loads(payload_json_str)
            except Exception:
                raw_payload = {}
        payload: Dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}

        if event_type == "session.created":
            s_id = session_id or payload.get("session_id")
            if not s_id:
                return False
            target_db.ensure_session(
                session_id=s_id,
                source=payload.get("source", "unknown"),
                model=payload.get("model"),
                model_config=payload.get("model_config"),
                system_prompt=payload.get("system_prompt"),
                user_id=payload.get("user_id"),
                session_key=payload.get("session_key"),
                chat_id=payload.get("chat_id"),
                chat_type=payload.get("chat_type"),
                thread_id=payload.get("thread_id"),
                parent_session_id=payload.get("parent_session_id"),
                cwd=payload.get("cwd"),
                profile_name=payload.get("profile_name"),
                git_repo_root=payload.get("git_repo_root"),
                origin_json=payload.get("origin_json"),
                display_name=payload.get("display_name"),
                transport_profile=payload.get("transport_profile"),
            )
            return True

        elif event_type == "session.title_updated":
            s_id = session_id or payload.get("session_id")
            if not s_id:
                return False
            target_db.set_session_title(s_id, payload.get("title", ""))
            return True

        elif event_type == "message.appended":
            s_id = session_id or payload.get("session_id")
            message_uid = payload.get("message_uid")
            if not s_id:
                return False

            # Idempotency check using (session_id, message_uid) WITHOUT filter active (Decision 3)
            if message_uid:
                existing = target_db._read_one(
                    "SELECT id FROM messages WHERE session_id = ? AND message_uid = ? LIMIT 1",
                    (s_id, message_uid),
                )
                if existing is not None:
                    return True  # Already projected, idempotent no-op

            # Restore offloaded fields from blob store if any were offloaded
            payload = restore_payload_fields(payload, blob_store=self.blob_store)

            # Resolve api_content from blob store if api_content_ref is a sha256 ref
            api_content_ref = event_dict.get("api_content_ref")
            api_content = payload.get("api_content")
            if not api_content and api_content_ref and str(api_content_ref).startswith("sha256:"):
                api_content = load_api_content(str(api_content_ref), blob_store=self.blob_store)

            target_db.append_message(
                session_id=s_id,
                role=payload.get("role", "user"),
                content=payload.get("content"),
                message_uid=message_uid,
                api_content=api_content,
                tool_name=payload.get("tool_name"),
                tool_calls=payload.get("tool_calls"),
                tool_call_id=payload.get("tool_call_id"),
                tool_call_uids=payload.get("tool_call_uids"),
                tool_call_uid=payload.get("tool_call_uid"),
                token_count=payload.get("token_count"),
                finish_reason=payload.get("finish_reason"),
                reasoning=payload.get("reasoning"),
                reasoning_content=payload.get("reasoning_content"),
                reasoning_details=payload.get("reasoning_details"),
                codex_reasoning_items=payload.get("codex_reasoning_items"),
                codex_message_items=payload.get("codex_message_items"),
                platform_message_id=payload.get("platform_message_id"),
                observed=payload.get("observed", False),
                effect_disposition=payload.get("effect_disposition"),
                timestamp=payload.get("timestamp"),
                display_kind=payload.get("display_kind"),
                display_metadata=payload.get("display_metadata"),
                _compressed_summary=bool(payload.get("_compressed_summary", False)),
            )
            return True

        elif event_type == "conversation.undone":
            s_id = session_id or payload.get("session_id")
            if not s_id:
                return False
            undone_uids = payload.get("undone_message_uids") or []
            target_mid = payload.get("target_message_id")

            def _do_undo(conn: sqlite3.Connection) -> None:
                if undone_uids:
                    placeholders = ",".join("?" for _ in undone_uids)
                    conn.execute(
                        f"UPDATE messages SET active = 0 WHERE session_id = ? AND message_uid IN ({placeholders})",
                        [s_id, *undone_uids],
                    )
                elif target_mid is not None:
                    conn.execute(
                        "UPDATE messages SET active = 0 WHERE session_id = ? AND id >= ?",
                        (s_id, target_mid),
                    )

                conn.execute(
                    "UPDATE sessions SET rewind_count = COALESCE(rewind_count, 0) + 1 WHERE id = ?",
                    (s_id,),
                )
                rows = conn.execute(
                    "SELECT tool_calls FROM messages WHERE session_id = ? AND active = 1", (s_id,)
                ).fetchall()
                msg_count = len(rows)
                tool_count = sum(1 for r in rows if r[0])
                conn.execute(
                    "UPDATE sessions SET message_count = ?, tool_call_count = ? WHERE id = ?",
                    (msg_count, tool_count, s_id),
                )

            target_db._execute_write(_do_undo)
            return True

        return True  # Unknown/future event types ignored by projection

    def replay_session(
        self,
        session_id: str,
        session_db: Optional[SessionDB] = None,
    ) -> int:
        """Replay all effective events for session_id in id ASC order into target SessionDB."""
        assert self.event_store is not None, "event_store is required for replay"
        target_db = session_db or self.session_db
        events = self.event_store.session_events(session_id)
        projected = 0
        for ev in events:
            if self.project_event(ev, target_db):
                projected += 1
        return projected

    def replay_pending(self, session_id: Optional[str] = None) -> int:
        """Catch up unprojected events from events.db into state.db.

        Advances last_projected_event_id.
        """
        assert self.event_store is not None, "event_store is required for replay"
        events = self.event_store.events_after(self.last_projected_event_id, limit=10000)
        if session_id:
            events = [e for e in events if e.get("session_id") == session_id]

        projected = 0
        for ev in events:
            if self.project_event(ev, self.session_db):
                self.last_projected_event_id = max(self.last_projected_event_id, ev["id"])
                projected += 1

        return projected
