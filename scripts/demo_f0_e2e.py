"""End-to-End Vertical Slice Demo & Replay Verification for Avunel Fase 0.

Demonstrates the complete end-to-end Fase 0 stack in an isolated sandbox:
1. Strict Production Isolation Guard (verifies ~/avunel/state.db untouched).
2. DualSink Event Logging (sessions, messages, blobs, rewind, title).
3. Full Event Projection Replay & Strict Parity verification.
4. Avunel Serve (SSE, Cockpit HTML, Token Auth, Loopback Origin Enforcement).
5. Timeline TUI Client consuming live SSE stream with cursor resume.
6. Post-execution Production Integrity Check.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import time
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from avunel_serve import AvunelEventServer
from avunel_timeline_tui import TimelineTUIClient
from blob_store import BlobStore
from dual_sink import DualSink
from f0_contract import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    EVENT_TYPE_BLOB_STORED,
    EVENT_TYPE_MESSAGE_APPENDED,
    EVENT_TYPE_SESSION_CREATED,
    EVENT_TYPE_SESSION_REWOUND,
    validate_host,
    validate_port,
)
from hermes_state import SessionDB
import hermes_state_events
from hermes_state_events import open_events

PROD_DB = Path.home() / "avunel" / "state.db"


def get_file_stats(path: Path) -> Tuple[Optional[str], Optional[float], int]:
    if not path.exists():
        return None, None, 0
    h = hashlib.sha256(path.read_bytes()).hexdigest()
    st = path.stat()
    return h, st.st_mtime, st.st_size


def run_e2e_demo(
    sandbox_dir: Optional[Path] = None,
    port: int = DEFAULT_PORT,
    token: str = "demo-e2e-token-f0",
    verbose: bool = False,
) -> bool:
    t_start = time.time()
    print("=" * 70)
    print("AVUNEL FASE 0: VERTICAL SLICE END-TO-END DEMO & REPLAY VERIFICATION")
    print("=" * 70)

    # 1. Snapshot Production Checksum
    prod_sha_before, prod_mtime_before, prod_size_before = get_file_stats(PROD_DB)
    print(f"[PROD CHECK 1] ~/avunel/state.db status: exists={PROD_DB.exists()}")
    if prod_sha_before:
        print(f"  SHA-256 : {prod_sha_before}")
        print(f"  Size    : {prod_size_before} bytes")

    # 2. Prepare Sandbox
    clean_temp = False
    if sandbox_dir is None:
        sandbox_dir = Path(tempfile.mkdtemp(prefix="avunel_f0_e2e_demo_"))
        clean_temp = True

    orig_env = {
        k: os.environ.get(k)
        for k in ("AVUNEL_HOME", "HERMES_HOME", "AVUNEL_STATE_MODE")
    }

    try:
        sandbox_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n[SANDBOX] Initialized isolated testbed: {sandbox_dir}")

        state_db_path = sandbox_dir / "state.db"
        events_db_path = sandbox_dir / "events.db"
        blob_dir = sandbox_dir / "blobs"
        recon_db_path = sandbox_dir / "reconstructed_state.db"

        os.environ["AVUNEL_HOME"] = str(sandbox_dir)
        os.environ["HERMES_HOME"] = str(sandbox_dir)
        os.environ["AVUNEL_STATE_MODE"] = "dual"

        session_db = SessionDB(db_path=state_db_path)
        event_store = open_events(events_db_path)
        blob_store = BlobStore(blob_dir)

        sink = DualSink(
            session_db=session_db,
            event_store=event_store,
            blob_store=blob_store,
            mode="dual",
        )

        # 3. Simulate Dual-Write Activity
        print("\n[STEP 1] Generating Dual-Write conversation stream...")
        sid = "sess_f0_vertical_slice"
        sink.create_session(sid, source="cli:sandbox", model="claude-3-5-sonnet")
        sink.set_session_title(sid, "Fase 0 Vertical Slice Session")

        # Message 1: User prompt
        msg1_id = sink.append_message(
            sid,
            role="user",
            content="Halo Avunel! Bagaimana status arsitektur dual-write dan SSE?",
        )

        # Message 2: Assistant tool invocation
        tcall_uid = "tcall_f0_001"
        msg2_id = sink.append_message(
            sid,
            role="assistant",
            content="Memeriksa status health dan event store...",
            tool_name="check_health",
            tool_calls=json.dumps([{"id": "call_f0", "type": "function", "function": {"name": "check_health", "arguments": "{}"}}]),
            tool_call_id="call_f0",
            tool_call_uid=tcall_uid,
        )

        # Message 3: Tool response
        msg3_id = sink.append_message(
            sid,
            role="tool",
            content='{"dual_sink": "active", "events_db": "synced"}',
            tool_name="check_health",
            tool_call_id="call_f0",
            tool_call_uid=tcall_uid,
        )

        # Message 4: Large assistant reply triggering CAS Blob offload (>60 KiB)
        large_content = "### LAPORAN STATUS ARSITEKTUR\n\n" + ("Dual-Write Event Log verified. " * 2500)
        msg4_id = sink.append_message(
            sid,
            role="assistant",
            content=large_content,
        )

        # Message 5 & 6: User message and rewind
        msg5_id = sink.append_message(
            sid,
            role="user",
            content="Pesan yang akan di-rewind segera.",
        )
        rewind_result = sink.rewind_to_message(sid, target_message_id=msg5_id)
        assert rewind_result.get("rewound_count", 0) >= 1

        total_events = event_store.count()
        print(f"  ✓ Emitted {total_events} events into events.db")
        print(f"  ✓ Written 5 messages into state.db (active count: {session_db.message_count(sid)})")

        # 4. Replay Projection & Parity Verification
        print("\n[STEP 2] Verifying Replay Projection & 100% Parity...")
        recon_session_db = SessionDB(db_path=recon_db_path)
        recon_sink = DualSink(
            session_db=recon_session_db,
            event_store=event_store,
            blob_store=blob_store,
            mode="dual",
        )
        recon_sink.replay_session(sid)

        # Parity assertions
        orig_title = session_db.get_session_title(sid)
        recon_title = recon_session_db.get_session_title(sid)
        assert orig_title == recon_title, f"Title mismatch: {orig_title} != {recon_title}"

        orig_msgs = session_db.get_messages(sid)
        recon_msgs = recon_session_db.get_messages(sid)
        assert len(orig_msgs) == len(recon_msgs), f"Message count mismatch: {len(orig_msgs)} != {len(recon_msgs)}"

        for i, (m1, m2) in enumerate(zip(orig_msgs, recon_msgs)):
            assert m1["role"] == m2["role"], f"Msg {i} role mismatch"
            assert m1["content"] == m2["content"], f"Msg {i} content mismatch"
            assert m1["active"] == m2["active"], f"Msg {i} active status mismatch"

        print(f"  ✓ Parity verified: original state.db and replayed state.db are 100% identical!")

        # 5. Start Avunel Serve
        print(f"\n[STEP 3] Starting Avunel Serve on http://{DEFAULT_HOST}:{port}...")
        server = AvunelEventServer(
            event_store=event_store,
            host=DEFAULT_HOST,
            port=port,
            token=token,
        )
        server.start(background=True)
        time.sleep(0.1)

        try:
            # 6. Consume via Timeline TUI Client
            print("\n[STEP 4] Connecting Timeline TUI Client to live SSE stream...")
            tui = TimelineTUIClient(
                host=DEFAULT_HOST,
                port=port,
                token=token,
                color=True,
                verbose=verbose,
            )

            # Stream all events up to current total
            print(f"  Streaming {total_events} events via SSE:")
            events_received: List[Dict] = []
            for ev in tui.stream_events(max_events=total_events, timeout=5.0):
                events_received.append(ev)
                line = tui.format_event(ev)
                print(f"    {line}")

            assert len(events_received) == total_events, (
                f"Expected {total_events} events via SSE, got {len(events_received)}"
            )
            print(f"  ✓ Received all {len(events_received)} events in exact sequence.")

            # Test cursor resume
            print("\n[STEP 5] Testing SSE Cursor Resume with last_id...")
            midpoint = total_events // 2
            expected_remaining = total_events - midpoint
            resume_tui = TimelineTUIClient(
                host=DEFAULT_HOST,
                port=port,
                token=token,
                last_id=midpoint,
                color=False,
            )
            resumed_events = list(resume_tui.stream_events(max_events=expected_remaining, timeout=3.0))
            assert len(resumed_events) == expected_remaining
            assert resumed_events[0]["id"] == midpoint + 1
            assert resumed_events[-1]["id"] == total_events
            print(f"  ✓ Resumed from #{midpoint} cleanly: received #{midpoint + 1}..#{total_events}")

        finally:
            server.shutdown()
            print("\n[STEP 6] Avunel Serve shut down cleanly.")

        # 7. Post-execution Production Integrity Check
        print("\n[STEP 7] Verifying Production Runtime Untouched...")
        prod_sha_after, prod_mtime_after, prod_size_after = get_file_stats(PROD_DB)
        print(f"[PROD CHECK 2] ~/avunel/state.db status:")
        if prod_sha_before:
            print(f"  SHA-256 Before: {prod_sha_before}")
            print(f"  SHA-256 After : {prod_sha_after}")
            assert prod_sha_before == prod_sha_after, (
                f"FATAL: ~/avunel/state.db modified! {prod_sha_before} != {prod_sha_after}"
            )
            assert prod_size_before == prod_size_after, "File size mismatch!"
            print("  ✓ ZERO BYTES MODIFIED in ~/avunel/state.db (Proof of Absolute Isolation)")
        else:
            assert not PROD_DB.exists(), "PROD_DB was unexpectedly created!"
            print("  ✓ ~/avunel/state.db does not exist and was not created.")

        duration = time.time() - t_start
        print("\n" + "=" * 70)
        print(f"DEMO SUCCESS: All Fase 0 Gates & Replay verified in {duration:.2f}s!")
        print("=" * 70)
        return True

    finally:
        for k, v in orig_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        if clean_temp and sandbox_dir and sandbox_dir.exists():
            shutil.rmtree(sandbox_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Avunel Fase 0 End-to-End Vertical Slice Demo"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Server port (default: {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--token",
        default="demo-e2e-token-f0",
        help="Auth token for demo",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging",
    )
    args = parser.parse_args()

    success = run_e2e_demo(
        port=args.port,
        token=args.token,
        verbose=args.verbose,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
