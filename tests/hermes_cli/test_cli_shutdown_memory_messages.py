"""Regression tests for #15165 (CLI sibling site) — CLI exit cleanup must
forward the agent's conversation transcript to ``shutdown_memory_provider``
so memory providers' ``on_session_end`` hooks see the real messages.

Before the fix, ``_run_cleanup`` called
``shutdown_memory_provider(getattr(agent, 'conversation_history', None) or [])``.
``AIAgent`` has no ``conversation_history`` attribute — so the ``or []``
branch always fired and providers got an empty list on CLI exit. This
mirrors the gateway bug fixed in the same commit (gateway/run.py uses
``_session_messages``, which IS set on ``AIAgent``).

The fix reads ``_session_messages`` (same attribute the gateway path uses)
with an ``isinstance(..., list)`` guard so MagicMock-based agents in
other tests keep their existing no-arg behaviour.
"""

from __future__ import annotations

import signal
import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch


@patch("hermes_cli.plugins.invoke_hook")
def test_cleanup_forwards_session_messages(mock_invoke_hook):
    """_run_cleanup forwards a populated ``_session_messages`` list."""
    import cli as cli_mod

    transcript = [
        {"role": "user", "content": "remember my dog is named Biscuit"},
        {"role": "assistant", "content": "Got it — Biscuit."},
    ]

    agent = MagicMock()
    agent.session_id = "cli-session-id"
    agent._session_messages = transcript

    cli_mod._active_agent_ref = agent
    cli_mod._cleanup_done = False
    try:
        cli_mod._run_cleanup()
    finally:
        cli_mod._active_agent_ref = None
        cli_mod._cleanup_done = False

    agent.shutdown_memory_provider.assert_called_once_with(transcript)






@patch("hermes_cli.plugins.invoke_hook")
def test_cleanup_provider_exception_is_swallowed(mock_invoke_hook):
    """A raising ``shutdown_memory_provider`` must not crash CLI exit."""
    import cli as cli_mod

    agent = MagicMock()
    agent.session_id = "cli-session-id"
    agent._session_messages = [{"role": "user", "content": "x"}]
    agent.shutdown_memory_provider.side_effect = RuntimeError("boom")

    cli_mod._active_agent_ref = agent
    cli_mod._cleanup_done = False
    try:
        cli_mod._run_cleanup()  # must not raise
    finally:
        cli_mod._active_agent_ref = None
        cli_mod._cleanup_done = False

    agent.shutdown_memory_provider.assert_called_once()










def _real_agent(db, session_id, session_messages):
    """Build the real persistence seam without the heavyweight LLM client."""
    from run_agent import AIAgent

    agent = object.__new__(AIAgent)
    agent._session_db = db
    agent._session_db_created = True
    agent.session_id = session_id
    agent.platform = "cli"
    agent.model = "test-model"
    agent._session_messages = session_messages
    agent._last_flushed_db_idx = 0
    agent._flushed_db_message_ids = set()
    agent._flushed_db_message_session_id = None
    agent._persist_disabled = False
    agent._cached_system_prompt = "test system prompt"
    agent._session_init_model_config = None
    agent._parent_session_id = None

    agent._pending_cli_user_message = None
    agent._session_persist_lock = threading.RLock()
    return agent




def test_cli_close_preflush_resumed_prefix_is_not_duplicated(tmp_path, monkeypatch):
    """A signal during the turn-start flush preserves the old DB prefix once.

    The pause is after ``_persist_session`` records its live snapshot but before
    its normal DB flush. The close helper must retain the distinct CLI baseline.
    """
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    import cli as cli_mod
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    session_id = "cli-close-preflush-resume"
    db.create_session(session_id=session_id, source="cli")
    loaded = [
        {"role": "user", "content": "old prompt"},
        {"role": "assistant", "content": "old answer"},
    ]
    for message in loaded:
        db.append_message(
            session_id=session_id,
            role=message["role"],
            content=message["content"],
        )

    live_messages = list(loaded) + [{"role": "user", "content": "new prompt"}]
    agent = _real_agent(db, session_id, [])
    entered_flush = threading.Event()
    release_flush = threading.Event()
    flush_calls = 0

    def _pause_before_flush(
        messages: list[dict[str, Any]],
        conversation_history: list[dict[str, Any]] | None = None,
    ) -> None:
        nonlocal flush_calls
        flush_calls += 1
        if flush_calls == 1:
            # The worker has assigned its snapshot and is now paused before its
            # regular DB write. The concurrent close call must stay live.
            agent._session_messages = messages
            entered_flush.set()
            assert release_flush.wait(timeout=5)
        from run_agent import AIAgent

        # Runtime accepts None; the stub keeps that optional contract explicit.
        return AIAgent._flush_messages_to_session_db(
            agent,
            messages,
            conversation_history if conversation_history is not None else [],
        )

    agent._flush_messages_to_session_db = _pause_before_flush
    worker = threading.Thread(
        target=lambda: agent._persist_session(live_messages, loaded),
        daemon=True,
    )
    worker.start()
    assert entered_flush.wait(timeout=5)

    cli = object.__new__(cli_mod.HermesCLI)
    cli.conversation_history = list(loaded) + [{"role": "user", "content": "ui prompt"}]
    cli.session_id = session_id
    cli.agent = agent
    close_started = threading.Event()
    close_finished = threading.Event()

    def _close_while_worker_flushes():
        close_started.set()
        cli._persist_active_session_before_close()
        close_finished.set()

    close_worker = threading.Thread(target=_close_while_worker_flushes, daemon=True)
    close_worker.start()
    assert close_started.wait(timeout=5)
    # The per-agent persistence lock holds the close flush until the normal
    # turn-start write has stamped its durable markers.
    assert not close_finished.wait(timeout=0.1)

    release_flush.set()
    worker.join(timeout=5)
    close_worker.join(timeout=5)
    assert not worker.is_alive()
    assert not close_worker.is_alive()

    stored = db.get_messages_as_conversation(session_id)
    assert [m["content"] for m in stored] == [
        "old prompt",
        "old answer",
        "new prompt",
    ]




def test_cli_close_hands_staged_user_marker_to_turn_start(tmp_path, monkeypatch):
    """A close before turn setup does not duplicate the CLI-staged user row."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    import cli as cli_mod
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    session_id = "cli-close-staged-user"
    db.create_session(session_id=session_id, source="cli")
    prefix = [
        {"role": "user", "content": "old prompt"},
        {"role": "assistant", "content": "old answer"},
    ]
    agent = _real_agent(db, session_id, prefix)
    agent._flush_messages_to_session_db(prefix, [])
    staged = {"role": "user", "content": "new prompt"}
    # `chat()` copies a completed agent transcript before it stages the next
    # user input, so close initially sees the prior agent snapshot only.
    cli_history = list(prefix) + [staged]
    agent._pending_cli_user_message = staged

    cli = object.__new__(cli_mod.HermesCLI)
    cli.conversation_history = cli_history
    cli.session_id = session_id
    cli.agent = agent

    # Close appends only the pending UI dict, while treating the durable prefix
    # as its baseline. Turn setup then reuses the marked dict without re-writing.
    cli._persist_active_session_before_close()
    assert staged["_db_persisted"] is True

    worker_messages = list(prefix) + [staged]
    agent._persist_session(worker_messages, prefix)

    stored = db.get_messages_as_conversation(session_id)
    assert [m["content"] for m in stored] == [
        "old prompt",
        "old answer",
        "new prompt",
    ]






def test_cli_close_uses_clean_override_for_shortened_pending_snapshot(tmp_path, monkeypatch):
    """Close retains the clean user text when its snapshot omits the prefix."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    import cli as cli_mod
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    session_id = "cli-close-shortened-noted-pending"
    db.create_session(session_id=session_id, source="cli")
    prefix = [
        {"role": "user", "content": "old prompt"},
        {"role": "assistant", "content": "old answer"},
    ]
    for message in prefix:
        db.append_message(
            session_id=session_id,
            role=message["role"],
            content=message["content"],
        )

    agent = _real_agent(db, session_id, [])
    staged = {"role": "user", "content": "[MODEL NOTE]\n\nnew prompt"}
    agent._pending_cli_user_message = staged
    # The normal worker index is relative to the full resumed history, while a
    # close before its first persistence flush sees only this staged dict.
    agent._persist_user_message_idx = len(prefix)
    agent._persist_user_message_override = "new prompt"
    agent._persist_user_message_timestamp = None

    cli = object.__new__(cli_mod.HermesCLI)
    cli.conversation_history = list(prefix) + [staged]
    cli.session_id = session_id
    cli.agent = agent

    cli._persist_active_session_before_close()

    assert [m["content"] for m in db.get_messages_as_conversation(session_id)] == [
        "old prompt",
        "old answer",
        "new prompt",
    ]
    assert staged["_db_persisted"] is True




def test_cli_close_builds_prompt_before_creating_first_session_row(tmp_path, monkeypatch):
    """First-turn close persistence must not leave a NULL prompt snapshot."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    import agent.conversation_loop as loop_mod
    import cli as cli_mod
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    session_id = "cli-close-first-turn"
    agent = _real_agent(db, session_id, [])
    agent._session_db_created = False
    agent._cached_system_prompt = None
    staged = {"role": "user", "content": "first prompt"}
    agent._pending_cli_user_message = staged

    def _build_prompt(target, _system_message, _history):
        target._cached_system_prompt = "close-built-system-prompt"

    monkeypatch.setattr(loop_mod, "_restore_or_build_system_prompt", _build_prompt)

    cli = object.__new__(cli_mod.HermesCLI)
    cli.conversation_history = [staged]
    cli.session_id = session_id
    cli.agent = agent

    cli._persist_active_session_before_close()

    session = db.get_session(session_id)
    assert session is not None
    assert session["system_prompt"] == "close-built-system-prompt"
    assert [m["content"] for m in db.get_messages_as_conversation(session_id)] == [
        "first prompt"
    ]


# ---------------------------------------------------------------------------
# Interrupt-during-drain regression (second Ctrl+C while on_session_end runs).
#
# Before the fix, ``_shutdown_agent_memory_provider`` ran the provider shutdown
# (``on_session_end`` -> LLM-bound extraction) INLINE on the main thread, so a
# second Ctrl+C raised KeyboardInterrupt in the main thread and unwound the
# already-started drain, dropping the session's tail. These tests reproduce that
# on the real path: the drain now runs on a worker, and the main thread waits,
# tolerating KeyboardInterrupt.
# ---------------------------------------------------------------------------


class _DrainAgent:
    """Minimal agent stub whose shutdown blocks like a real LLM-bound drain."""

    def __init__(self, *, block_s: float, fail: bool = False, messages: Any = ...):
        self.session_id = "drain-session"
        self._session_messages = (
            [{"role": "user", "content": "besok ke kebun raya"}] if messages is ... else messages
        )
        self._memory_manager = None  # skip flush_pending
        self._block_s = block_s
        self._fail = fail
        self.entered = threading.Event()
        self.completed = threading.Event()
        self.calls = []

    def shutdown_memory_provider(self, messages=None):
        self.calls.append(messages)
        self.entered.set()
        time.sleep(self._block_s)
        if self._fail:
            raise RuntimeError("provider boom")
        self.completed.set()


def _fire_sigint_after(delay_s: float, count: int = 1) -> threading.Thread:
    """Deliver ``count`` SIGINTs to the process after ``delay_s`` (raises KeyboardInterrupt
    in the main thread — same mechanism as the CLI's signal handler)."""
    def _run():
        time.sleep(delay_s)
        for _ in range(count):
            signal.raise_signal(signal.SIGINT)
            time.sleep(0.2)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t


def test_shutdown_survives_interrupt_during_drain():
    """A KeyboardInterrupt mid-drain must NOT abort the already-started drain."""
    import hermes_cli.cli_shutdown as cs

    agent = _DrainAgent(block_s=1.5)
    # Fire two SIGINTs during the drain window (double Ctrl+C).
    _fire_sigint_after(0.4, count=2)

    cs._shutdown_agent_memory_provider(agent)

    # The drain ran to completion despite the interrupts landing on the main thread.
    assert agent.entered.is_set(), "drain never started"
    assert agent.completed.is_set(), "drain was aborted by the interrupt (regression)"
    assert agent.calls == [agent._session_messages]


def test_shutdown_bounded_when_drain_wedges(monkeypatch):
    """A wedged drain must not hang the main thread forever (bounded wait)."""
    import hermes_cli.cli_shutdown as cs

    monkeypatch.setattr(cs, "_MEMORY_SHUTDOWN_JOIN_TIMEOUT_S", 0.5)
    agent = _DrainAgent(block_s=5.0)  # never finishes within the budget

    t0 = time.monotonic()
    cs._shutdown_agent_memory_provider(agent)
    elapsed = time.monotonic() - t0

    assert agent.entered.is_set(), "drain never started"
    assert not agent.completed.is_set(), "worker should still be running (wedged)"
    assert elapsed < 3.0, f"bounded wait did not return promptly: {elapsed:.2f}s"


def test_shutdown_worker_failure_is_swallowed():
    """A raising provider must not crash cleanup, and the wait must still return."""
    import hermes_cli.cli_shutdown as cs

    agent = _DrainAgent(block_s=0.1, fail=True)
    # must not raise
    cs._shutdown_agent_memory_provider(agent)
    assert agent.entered.is_set()


def test_shutdown_forwards_messages_and_no_arg_fallback():
    """Transcript is forwarded when present; no-arg fallback otherwise."""
    import hermes_cli.cli_shutdown as cs

    agent = _DrainAgent(block_s=0.05)
    cs._shutdown_agent_memory_provider(agent)
    assert agent.calls == [agent._session_messages]

    agent2 = _DrainAgent(block_s=0.05, messages=None)  # no transcript
    cs._shutdown_agent_memory_provider(agent2)
    assert agent2.calls == [None]
