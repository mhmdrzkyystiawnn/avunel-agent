"""Integration test: the temporal guard hook actually blocks a memory write.

    HERMES_HOME=<tmp> PYTHONPATH=<repo> <hermes-venv>/bin/python3 test_guard_integration.py

Registers the REAL provider through the real plugin context
(``_ProviderCollector``), then drives the REAL ``pre_tool_call`` dispatch
(``hermes_cli.plugins._dispatch_pre_tool_call_hooks``) to prove the hook fires
and vetoes a dated ``memory`` write — not just that the pure predicate works.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_failures = 0


def check(label: str, cond: bool, detail: str = "") -> None:
    global _failures
    if not cond:
        _failures += 1
    print(f"{'ok  ' if cond else 'FAIL'} {label}{('  -- ' + detail) if detail and not cond else ''}")


def main() -> int:
    os.environ.setdefault("HERMES_HOME", os.path.abspath(os.path.dirname(__file__)))

    import __init__ as temporal_pkg  # the provider package (this dir)
    from plugins.memory import _ProviderCollector
    from hermes_cli import plugins as plugins_mod

    # Register through the REAL memory-provider collector (same path as discovery).
    collector = _ProviderCollector("temporal", register_skills=False)
    collector.collect(temporal_pkg.register)

    dispatch = plugins_mod._dispatch_pre_tool_call_hooks

    # 1) dated memory write -> blocked, with a message
    block_msg, _ = dispatch("memory", {"action": "add", "target": "memory",
                                       "content": "besok aku mau ke kebun raya sama pacar"})
    check("integration: dated memory write is BLOCKED", isinstance(block_msg, str) and bool(block_msg),
          repr(block_msg))
    check("integration: block message mentions temporal",
          isinstance(block_msg, str) and "temporal" in block_msg.lower())

    # 2) non-temporal memory write -> allowed
    allow_msg, _ = dispatch("memory", {"action": "add", "target": "memory",
                                       "content": "User prefers concise responses"})
    check("integration: non-temporal memory write is ALLOWED", allow_msg is None, repr(allow_msg))

    # 3) target=user with a date -> allowed (identity/profile)
    user_msg, _ = dispatch("memory", {"action": "add", "target": "user",
                                      "content": "ulang tahun user 12 september"})
    check("integration: target=user is ALLOWED", user_msg is None, repr(user_msg))

    # 4) unrelated tool -> unaffected
    other_msg, _ = dispatch("read_file", {"path": "besok.txt"})
    check("integration: unrelated tool unaffected", other_msg is None, repr(other_msg))

    print(f"\n{'ALL PASSED' if _failures == 0 else str(_failures) + ' FAILED'}")
    return 1 if _failures else 0


if __name__ == "__main__":
    sys.exit(main())
