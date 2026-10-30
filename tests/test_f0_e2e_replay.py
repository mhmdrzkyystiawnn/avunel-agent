"""Pytest suite for Fase 0 End-to-End Replay & Vertical Slice Integration."""

from __future__ import annotations

from pathlib import Path
import pytest

from scripts.demo_f0_e2e import run_e2e_demo


def test_f0_e2e_vertical_slice_integration(tmp_path: Path) -> None:
    """Run complete Fase 0 vertical slice in isolated sandbox directory."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        ephemeral_port = s.getsockname()[1]

    success = run_e2e_demo(
        sandbox_dir=tmp_path / "sandbox",
        port=ephemeral_port,
        token="test-e2e-suite-token",
        verbose=False,
    )
    assert success is True
