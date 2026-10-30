"""Avunel Fase 0 Benchmark Suite (Gate G0-Release).

Measures:
1. Import Throughput (events/sec) from JSON fixture.
2. DualSink Projection Replay Throughput (events/sec, p50/p95 latency).
3. SSE Delivery Latency (p50/p95 ms) via AvunelEventServer.
4. Storage Overhead (events.db vs state.db vs CAS blobs).
5. Memory Peak (RSS).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import socket
import sys
import tempfile
import time
from typing import Any, Dict, List, Tuple
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from avunel_serve import AvunelEventServer
from blob_store import BlobStore
from dual_sink import DualSink
from f0_contract import DEFAULT_HOST, DEFAULT_PORT
from hermes_state import SessionDB
import hermes_state_events
from hermes_state_events import open_events


def get_system_facts() -> Dict[str, str]:
    return {
        "platform": platform.platform(),
        "python_version": sys.version.split()[0],
        "machine": platform.machine(),
        "processor": platform.processor(),
    }


def get_peak_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # On Linux, ru_maxrss is in KiB
    return usage.ru_maxrss / 1024.0


def run_benchmark(fixture_path: Path, repeat_factor: int = 50) -> Dict[str, Any]:
    print("=" * 70)
    print("AVUNEL FASE 0 BENCHMARK (Gate G0-Release)")
    print("=" * 70)

    facts = get_system_facts()
    for k, v in facts.items():
        print(f"  {k:<16}: {v}")

    fixture_bytes = fixture_path.read_bytes()
    fixture_hash = hashlib.sha256(fixture_bytes).hexdigest()
    raw_fixture_events = json.loads(fixture_bytes)
    print(f"  fixture_file    : {fixture_path.name}")
    print(f"  fixture_sha256  : {fixture_hash[:16]}...")
    print(f"  raw_events      : {len(raw_fixture_events)}")
    print(f"  repeat_factor   : {repeat_factor}x (target ~{len(raw_fixture_events) * repeat_factor} events)")

    sandbox_dir = Path(tempfile.mkdtemp(prefix="avunel_f0_benchmark_"))

    try:
        events_db_path = sandbox_dir / "events.db"
        state_db_path = sandbox_dir / "state.db"
        blob_dir = sandbox_dir / "blobs"

        event_store = open_events(events_db_path)
        session_db = SessionDB(db_path=state_db_path)
        blob_store = BlobStore(blob_dir)

        # ---------------------------------------------------------
        # 1. Import Throughput
        # ---------------------------------------------------------
        print("\n[1/4] Measuring Event Import Throughput...")
        t0 = time.perf_counter()
        total_imported = 0

        # Emit repeated batches to get statistical significance
        with event_store.acquire() as conn:
            for rep in range(repeat_factor):
                for ev in raw_fixture_events:
                    payload = dict(ev["payload"])
                    p_type = ev["type"]
                    uid = None
                    if p_type.startswith("message."):
                        uid = f"bench_msg_{rep}_{total_imported}"
                        payload["message_uid"] = uid

                    ts = int(ev.get("ts") or (payload.get("timestamp", time.time()) * 1000))

                    # Pre-serialize to measure pure store ingestion
                    event_store.emit(
                        type=p_type,
                        ts=ts,
                        v=ev.get("v", 1),
                        payload=payload,
                        session_id=f"bench_sess_{rep % 5}",
                        actor=ev.get("actor", "cli:bench"),
                        api_content_ref=f"msg:{uid}" if uid else None,
                        redact=False,
                    )
                    total_imported += 1

        t_import = time.perf_counter() - t0
        import_rate = total_imported / t_import if t_import > 0 else 0
        print(f"  -> Imported {total_imported} events in {t_import:.4f}s ({import_rate:.1f} events/sec)")

        # ---------------------------------------------------------
        # 2. DualSink Projection Replay & Latency
        # ---------------------------------------------------------
        print("\n[2/4] Measuring Projection Replay Latency & Throughput...")
        sink = DualSink(
            session_db=session_db,
            event_store=event_store,
            blob_store=blob_store,
            mode="dual",
        )

        latencies_ms: List[float] = []
        t0_replay = time.perf_counter()

        for sess_idx in range(min(5, repeat_factor)):
            s_id = f"bench_sess_{sess_idx}"
            s_t0 = time.perf_counter()
            sink.replay_session(s_id)
            latencies_ms.append((time.perf_counter() - s_t0) * 1000.0)

        t_replay_total = time.perf_counter() - t0_replay
        replay_rate = total_imported / t_replay_total if t_replay_total > 0 else 0

        latencies_ms.sort()
        p50 = latencies_ms[len(latencies_ms) // 2] if latencies_ms else 0.0
        p95 = latencies_ms[int(len(latencies_ms) * 0.95)] if latencies_ms else 0.0

        print(f"  -> Replayed sessions in {t_replay_total:.4f}s ({replay_rate:.1f} events/sec)")
        print(f"  -> Per-session projection latency: p50={p50:.2f}ms, p95={p95:.2f}ms")

        # ---------------------------------------------------------
        # 3. SSE Delivery Latency
        # ---------------------------------------------------------
        print("\n[3/4] Measuring SSE Delivery Latency via Avunel Serve...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((DEFAULT_HOST, 0))
            test_port = s.getsockname()[1]

        token = "bench-token-secret"
        server = AvunelEventServer(
            event_store=event_store,
            host=DEFAULT_HOST,
            port=test_port,
            token=token,
        )
        server.start(background=True)
        time.sleep(0.05)

        sse_latencies_ms: List[float] = []
        try:
            # Measure time to connect and receive initial chunk of 50 events
            for _ in range(5):
                t_req = time.perf_counter()
                url = f"http://{DEFAULT_HOST}:{test_port}/api/events/stream?token={token}&last_id=0"
                req = Request(url)
                with urlopen(req, timeout=5.0) as resp:
                    # Read first event line
                    line = resp.readline()
                    t_first_byte = (time.perf_counter() - t_req) * 1000.0
                    sse_latencies_ms.append(t_first_byte)

            sse_latencies_ms.sort()
            sse_p50 = sse_latencies_ms[len(sse_latencies_ms) // 2]
            sse_p95 = sse_latencies_ms[int(len(sse_latencies_ms) * 0.95)]
            print(f"  -> SSE TTFB (first event delivery): p50={sse_p50:.2f}ms, p95={sse_p95:.2f}ms")
        finally:
            server.shutdown()

        # ---------------------------------------------------------
        # 4. Storage & Memory Overhead
        # ---------------------------------------------------------
        print("\n[4/4] Measuring Storage & Memory Overhead...")
        events_db_size = events_db_path.stat().st_size if events_db_path.exists() else 0
        state_db_size = state_db_path.stat().st_size if state_db_path.exists() else 0
        peak_rss = get_peak_rss_mb()

        print(f"  -> events.db size  : {events_db_size / 1024:.1f} KiB")
        print(f"  -> state.db size   : {state_db_size / 1024:.1f} KiB")
        print(f"  -> Peak RSS Memory : {peak_rss:.1f} MiB")

        summary = {
            "facts": facts,
            "fixture": {
                "name": fixture_path.name,
                "sha256": fixture_hash,
                "total_events": total_imported,
            },
            "import": {
                "duration_seconds": round(t_import, 4),
                "events_per_second": round(import_rate, 1),
            },
            "replay": {
                "duration_seconds": round(t_replay_total, 4),
                "events_per_second": round(replay_rate, 1),
                "session_latency_p50_ms": round(p50, 2),
                "session_latency_p95_ms": round(p95, 2),
            },
            "sse": {
                "ttfb_p50_ms": round(sse_p50, 2),
                "ttfb_p95_ms": round(sse_p95, 2),
            },
            "storage": {
                "events_db_kib": round(events_db_size / 1024.0, 1),
                "state_db_kib": round(state_db_size / 1024.0, 1),
                "peak_rss_mib": round(peak_rss, 1),
            },
        }

        print("\n" + "=" * 70)
        print("BENCHMARK SUMMARY (JSON):")
        print(json.dumps(summary, indent=2))
        print("=" * 70)
        return summary

    finally:
        shutil.rmtree(sandbox_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Avunel Fase 0 Benchmark Suite")
    parser.add_argument(
        "--fixture",
        type=Path,
        default=REPO_ROOT / "fixtures" / "synthetic_demo_events.json",
        help="Path to demo events fixture",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=50,
        help="Repeat multiplier for synthetic events (default: 50)",
    )
    args = parser.parse_args()

    run_benchmark(fixture_path=args.fixture, repeat_factor=args.repeat)


if __name__ == "__main__":
    main()
