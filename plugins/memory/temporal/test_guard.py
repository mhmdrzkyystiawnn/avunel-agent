"""Guard tests — temporal facts must be vetoed from the built-in memory store.

    HERMES_HOME=<tmp> PYTHONPATH=<repo> <hermes-venv>/bin/python3 test_guard.py

Offline, no network. Exercises the pure `guard` module: the `pre_tool_call`
decision for a `memory` write.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import guard  # noqa: E402

_failures = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global _failures
    if not cond:
        _failures += 1
    print(f"{'ok  ' if cond else 'FAIL'} {label}{('  -- ' + detail) if detail and not cond else ''}")


def _mem(content: str, *, target: str = "memory", action: str = "add") -> dict:
    return {"action": action, "target": target, "content": content}


def main() -> int:
    # --- BLOCK: strong dated facts in a memory write ---
    blocks = [
        "besok aku mau ke kebun raya sama pacar",
        "lusa ada janji ke dokter",
        "kemarin hujan deras di jogja",
        "tanggal 12 september ada acara kantor",
        "12 september gue ada meeting",
        "2 hari lalu gue kecelakaan motor",
        "minggu depan ke bogor",
        "bulan lalu gue ke bali",
        "tahun depan mau pindah rumah",
        "tadi malam kurang tidur",
    ]
    for text in blocks:
        check(f"block: {text!r}", guard.should_block_memory_write("memory", _mem(text)) is True)

    # --- ALLOW: non-temporal / standing conventions / identity ---
    allows = [
        "User prefers concise responses",
        "Nama user adalah Iki",
        "User tinggal di Yogyakarta",
        "meeting tiap Jumat",                 # standing convention, bare weekday
        "libur tiap minggu",                  # bare 'minggu' — NOT a dated fact
        "gaji masuk tiap bulan",              # bare 'bulan'
        "User suka kopi tanpa gula",
    ]
    for text in allows:
        check(f"allow (non-temporal): {text!r}",
              guard.should_block_memory_write("memory", _mem(text)) is False)

    # --- target=user never blocked (identity/profile) ---
    check("allow: target=user with a date",
          guard.should_block_memory_write("memory", _mem("ulang tahun user 12 september", target="user")) is False)

    # --- remove is never blocked ---
    check("allow: remove action with a date",
          guard.should_block_memory_write("memory", _mem("besok ke kebun raya", action="remove")) is False)

    # --- other tools never blocked ---
    check("allow: non-memory tool",
          guard.should_block_memory_write("read_file", {"path": "besok.txt"}) is False)

    # --- batch operations ---
    batch_temporal = {"action": "batch", "target": "memory", "operations": [
        {"action": "add", "content": "User suka kopi"},
        {"action": "add", "content": "besok ke kebun raya"},
    ]}
    check("block: batch with one dated op", guard.should_block_memory_write("memory", batch_temporal) is True)

    batch_clean = {"action": "batch", "target": "memory", "operations": [
        {"action": "add", "content": "User suka kopi"},
        {"action": "remove", "old_text": "besok ke kebun raya"},
    ]}
    check("allow: batch with only non-temporal add + a remove",
          guard.should_block_memory_write("memory", batch_clean) is False)

    # --- replace with new_text (alias for content) ---
    check("block: replace new_text dated",
          guard.should_block_memory_write("memory", {"action": "replace", "target": "memory",
                                                     "old_text": "x", "new_text": "lusa ke dokter"}) is True)

    # --- fail-open on malformed input ---
    for bad in (None, [], "besok", 123):
        check(f"fail-open: malformed args {bad!r}",
              guard.should_block_memory_write("memory", bad) is False)
    check("fail-open: None tool_name",
          guard.should_block_memory_write(None, _mem("besok")) is False)

    # --- looks_temporal_fact direct ---
    check("looks_temporal_fact: 'besok'", guard.looks_temporal_fact("besok") is True)
    check("looks_temporal_fact: bare 'minggu'", guard.looks_temporal_fact("minggu") is False)
    check("looks_temporal_fact: empty", guard.looks_temporal_fact("") is False)

    print(f"\n{'ALL PASSED' if _failures == 0 else str(_failures) + ' FAILED'}")
    return 1 if _failures else 0


if __name__ == "__main__":
    sys.exit(main())
