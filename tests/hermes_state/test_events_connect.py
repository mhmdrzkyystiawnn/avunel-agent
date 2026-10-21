"""Tests for connection management, transaction handling, and WAL concurrency (F0-05).

Decisions and invariants tested:
* PRAGMAs enforced explicitly on every connection:
    - journal_mode = WAL
    - synchronous = FULL (2)
    - busy_timeout = 5000 ms
    - foreign_keys = ON (1)
    - wal_autocheckpoint = 1000 (SQLite default)
* store.acquire() is re-entrant on the same thread:
    - Top-level executes BEGIN IMMEDIATE, commits on exit, rolls back on exception.
    - Nested calls yield the active connection without nested BEGIN statements.
    - An exception in a nested block rolls back all writes made in the top-level transaction.
* emit() guards against Python bool booby-trap:
    - ts=True/False rejected with TypeError.
    - v=True/False rejected with ValueError.
* WAL lock coordination:
    - 2 concurrent threads succeed under default 5000ms busy_timeout when one holds the lock briefly.
    - A connection with a short busy_timeout raises OperationalError('database is locked') when lock is held.
* Module-level acquire(db_path):
    - Path canonicalization with resolve() and thread-safe registry returning shared EventStore instance.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

import pytest

import hermes_state_events
from hermes_state_events import EventStore, _clear_registry, acquire


@pytest.fixture(autouse=True)
def clean_registry():
    _clear_registry()
    yield
    _clear_registry()


@pytest.fixture
def store(tmp_path: Path) -> EventStore:
    return EventStore(tmp_path / "events.db")


# ── 1. PRAGMA enforcement ───────────────────────────────────────────────────


def test_pragmas_enforced_explicitly(store: EventStore):
    """Verify all explicit PRAGMAs on the connection (F0-05 & F0-02)."""
    conn = store._conn()
    journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    synchronous = conn.execute("PRAGMA synchronous").fetchone()[0]
    busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    wal_autocheckpoint = conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0]

    assert str(journal_mode).lower() == "wal"
    assert synchronous == 2  # 2 == FULL
    assert busy_timeout == 5000
    assert foreign_keys == 1  # 1 == ON
    assert wal_autocheckpoint == 1000


# ── 2. Top-level acquire() transaction semantics ────────────────────────────


def test_acquire_context_manager_commits_on_success(store: EventStore):
    """Writes inside store.acquire() commit upon normal block exit."""
    with store.acquire() as conn:
        conn.execute(
            """
            INSERT INTO events (ts, type, v, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (1000, "custom.test", 1, "{}"),
        )

    # Read back to confirm committed
    assert store.count() == 1
    event = store.get_event(1)
    assert event is not None
    assert event["type"] == "custom.test"
    assert not store._conn().in_transaction


def test_acquire_context_manager_rolls_back_on_exception(store: EventStore):
    """Writes inside store.acquire() roll back if an exception escapes."""
    with pytest.raises(RuntimeError, match="deliberate failure"):
        with store.acquire() as conn:
            conn.execute(
                """
                INSERT INTO events (ts, type, v, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (1000, "custom.failed", 1, "{}"),
            )
            raise RuntimeError("deliberate failure")

    assert store.count() == 0
    assert not store._conn().in_transaction


def test_acquire_base_exception_rolls_back(store: EventStore):
    """BaseException (e.g. KeyboardInterrupt) must roll back and leave transaction clean."""
    with pytest.raises(KeyboardInterrupt):
        with store.acquire() as conn:
            conn.execute(
                """
                INSERT INTO events (ts, type, v, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (1000, "custom.interrupted", 1, "{}"),
            )
            raise KeyboardInterrupt("simulated SIGINT")

    assert store.count() == 0
    assert not store._conn().in_transaction


# ── 3. Nested acquire() and emit() re-entrancy ──────────────────────────────


def test_nested_acquire_and_emit_atomic_rollback(store: EventStore):
    """Nested acquire() calls inside store.acquire() share transaction and roll back atomically."""
    with pytest.raises(ValueError, match="abort all writes"):
        with store.acquire():
            id1 = store.emit("custom.step1", ts=1001, payload={"step": 1})
            assert id1 == 1
            id2 = store.emit("custom.step2", ts=1002, payload={"step": 2})
            assert id2 == 2
            # Deliberately raise inside the outer transaction block
            raise ValueError("abort all writes")

    # Both emits must have been rolled back cleanly
    assert store.count() == 0
    assert list(store.all_events()) == []
    assert not store._conn().in_transaction


def test_nested_acquire_and_emit_atomic_commit(store: EventStore):
    """Multiple emit() calls inside outer store.acquire() commit atomically."""
    with store.acquire():
        id1 = store.emit("custom.step1", ts=1001, payload={"step": 1})
        id2 = store.emit("custom.step2", ts=1002, payload={"step": 2})
        assert id1 == 1
        assert id2 == 2

    assert store.count() == 2
    events = list(store.all_events())
    assert len(events) == 2
    assert events[0]["type"] == "custom.step1"
    assert events[1]["type"] == "custom.step2"
    assert not store._conn().in_transaction


def test_nested_acquire_savepoint_rollback_when_inner_exception_caught(store: EventStore):
    """Inner acquire() rollback via SAVEPOINT does not leak to outer commit if caught."""
    with store.acquire():
        id1 = store.emit("custom.outer1", ts=1001, payload={"step": 1})
        try:
            with store.acquire():
                store.emit("custom.inner", ts=1002, payload={"step": 2})
                raise RuntimeError("inner failure swallowed by outer")
        except RuntimeError:
            pass
        id2 = store.emit("custom.outer2", ts=1003, payload={"step": 3})

    assert store.count() == 2
    events = list(store.all_events())
    assert len(events) == 2
    assert events[0]["id"] == id1
    assert events[0]["type"] == "custom.outer1"
    assert events[1]["id"] == id2
    assert events[1]["type"] == "custom.outer2"
    # The inner event was rolled back cleanly by SAVEPOINT
    assert not any(e["type"] == "custom.inner" for e in events)
    assert not store._conn().in_transaction


# ── 4. Guard against bool type confusion on ts and v ────────────────────────


@pytest.mark.parametrize("invalid_ts", [True, False])
def test_emit_rejects_bool_for_ts(store: EventStore, invalid_ts):
    """ts=True or ts=False must be rejected with TypeError (not accepted as int 1/0)."""
    with pytest.raises(TypeError, match="ts must be an int"):
        store.emit("custom.test", ts=invalid_ts, payload={})


@pytest.mark.parametrize("invalid_v", [True, False])
def test_emit_rejects_bool_for_v(store: EventStore, invalid_v):
    """v=True or v=False must be rejected with ValueError (not accepted as int 1/0)."""
    with pytest.raises(ValueError, match="v must be a positive integer"):
        store.emit("custom.test", ts=1000, v=invalid_v, payload={})


# ── 5. WAL concurrency and busy_timeout coordination ────────────────────────


def test_concurrent_threads_wal_busy_timeout_coordination(store: EventStore):
    """Thread 2 waits for Thread 1's lock release under 5000ms busy_timeout and succeeds."""
    barrier = threading.Barrier(2)
    thread1_started = threading.Event()
    results: dict[str, Optional[int]] = {"t1": None, "t2": None}
    errors: list[Exception] = []

    def thread1_worker():
        try:
            barrier.wait()
            with store.acquire() as conn:
                thread1_started.set()
                # Hold the lock briefly (0.2s)
                time.sleep(0.2)
                cursor = conn.execute(
                    "INSERT INTO events (ts, type, v, payload_json) VALUES (?, ?, ?, ?)",
                    (2001, "custom.thread1", 1, "{}"),
                )
                results["t1"] = cursor.lastrowid
        except Exception as e:
            errors.append(e)

    def thread2_worker():
        try:
            barrier.wait()
            # Ensure thread 1 has entered acquire() and holds the SQLite WAL write lock
            assert thread1_started.wait(timeout=1.0), "Thread 1 failed to acquire lock in time"
            # Thread 2 attempts to emit — will wait on busy_timeout (5000ms) for Thread 1
            results["t2"] = store.emit("custom.thread2", ts=2002, payload={})
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=thread1_worker)
    t2 = threading.Thread(target=thread2_worker)
    t1.start()
    t2.start()
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)

    assert not errors, f"Concurrent thread errors: {errors}"
    assert results["t1"] is not None
    assert results["t2"] is not None
    assert store.count() == 2


def test_concurrent_threads_lock_timeout_exceeded(store: EventStore):
    """A connection with short busy_timeout raises OperationalError when write lock is held."""
    lock_held = threading.Event()
    release_lock = threading.Event()
    thread2_error: list[Exception] = []

    def holder_worker():
        with store.acquire():
            lock_held.set()
            release_lock.wait(timeout=2.0)

    t1 = threading.Thread(target=holder_worker)
    t1.start()

    try:
        assert lock_held.wait(timeout=1.0), "Holder failed to acquire lock"
        # Open separate raw connection with tiny timeout (50ms)
        conn = sqlite3.connect(str(store.db_path), isolation_level=None, timeout=0.05)
        try:
            conn.execute("PRAGMA busy_timeout=50")
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                conn.execute("BEGIN IMMEDIATE")
        finally:
            conn.close()
    finally:
        release_lock.set()
        t1.join(timeout=2.0)


# ── 6. Thread-safety & Path canonicalization in module acquire() ──────────────


def test_module_acquire_returns_shared_instance_per_path(tmp_path: Path):
    """acquire(path) resolves equivalent paths and returns the shared EventStore instance."""
    db_file = tmp_path / "events.db"
    store1 = acquire(db_file)
    store2 = acquire(str(db_file))
    # Equivalent path with redundant parts
    dot_path = tmp_path / "sub" / ".." / "events.db"
    store3 = acquire(dot_path)

    assert store1 is store2
    assert store2 is store3

    # Different path returns distinct instance
    other_file = tmp_path / "other_events.db"
    store_other = acquire(other_file)
    assert store_other is not store1


def test_multi_thread_connection_isolation(store: EventStore):
    """Each thread gets its own SQLite connection instance via thread-local storage."""
    connections: list[sqlite3.Connection] = []

    def worker():
        connections.append(store._conn())

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(connections) == 2
    assert connections[0] is not connections[1]
    assert connections[0] is not store._conn()
