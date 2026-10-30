"""Pytest suite for Gate G0a-2: Dual-write runtime parity and isolation guard."""

from __future__ import annotations

import os
from pathlib import Path
import pytest

from scripts.verify_g0a2 import run_g0a2_verification, PROD_DB, get_file_stats
from scripts.dev_launcher import enforce_isolation_guard


def test_isolation_guard_triggers_on_prod_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolation guard must exit if AVUNEL_HOME points to production directory."""
    monkeypatch.setenv("AVUNEL_HOME", str(Path.home() / "avunel"))
    with pytest.raises(SystemExit):
        enforce_isolation_guard()

    monkeypatch.setenv("AVUNEL_HOME", str(Path.home() / "avunel" / "subdir"))
    with pytest.raises(SystemExit):
        enforce_isolation_guard()


def test_g0a2_dual_write_runtime_smoke_flow_and_isolation(tmp_path: Path) -> None:
    """Run full G0a-2 dual-write smoke flow and verify production isolation."""
    sandbox_dir = tmp_path / "sandbox_g0a2"
    passed = run_g0a2_verification(sandbox_dir)
    assert passed is True
