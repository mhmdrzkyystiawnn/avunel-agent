"""Avunel data-home resolution: AVUNEL_HOME > HERMES_HOME > legacy dir > default.

Three seams are covered, all of them real code paths, none of them mocked:

* ``hermes_constants`` — the Python resolver every runtime reader funnels through.
* ``scripts/install.sh`` — sourced for its functions (``--manifest``), so the
  resolution runs without downloading a toolchain.
* ``setup-hermes.sh`` — its side-effect-free ``--print-resolved-home`` contract.

The contract under test: AVUNEL_HOME is canonical and wins over HERMES_HOME when
both are set; HERMES_HOME still works alone; a machine with only a legacy
``~/.hermes`` keeps using it; a fresh machine gets ``~/.avunel``.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.platforms("posix")


def _minimal_env(home: Path, **overrides: str) -> dict:
    """A clean environment: no inherited HERMES_HOME/AVUNEL_HOME can leak in."""
    env = {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
    }
    env.update(overrides)
    return env


def _install_sh_home(home: Path, **overrides: str) -> str:
    """Source install.sh for its functions and print the resolved HERMES_HOME."""
    bash = shutil.which("bash")
    assert bash, "the installer contract requires Bash"
    script = (
        f'source "{(ROOT / "scripts" / "install.sh").as_posix()}" --manifest >/dev/null\n'
        'printf "%s" "$HERMES_HOME"\n'
    )
    result = subprocess.run(
        [bash, "-c", script],
        env=_minimal_env(home, **overrides),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def _setup_sh_home(home: Path, **overrides: str) -> str:
    """Run setup-hermes.sh's side-effect-free resolver and parse its JSON."""
    bash = shutil.which("bash")
    assert bash, "the setup contract requires Bash"
    result = subprocess.run(
        [bash, str(ROOT / "setup-hermes.sh"), "--print-resolved-home"],
        env=_minimal_env(home, **overrides),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)["hermes_home"]


# ---------------------------------------------------------------------------
# install.sh
# ---------------------------------------------------------------------------

def test_install_sh_avunel_home_wins_over_hermes_home(tmp_path):
    """Both env vars set: AVUNEL_HOME is canonical and must not be overridden."""
    assert _install_sh_home(tmp_path, AVUNEL_HOME="/canonical", HERMES_HOME="/legacy") == "/canonical"


def test_install_sh_fresh_install_defaults_to_avunel(tmp_path):
    """No env, nothing on disk: a brand-new install gets ~/.avunel."""
    assert _install_sh_home(tmp_path) == str(tmp_path / ".avunel")


def test_install_sh_reuses_lone_legacy_home(tmp_path):
    """Only ~/.hermes exists: it is reused so existing data is never orphaned."""
    (tmp_path / ".hermes").mkdir()
    assert _install_sh_home(tmp_path) == str(tmp_path / ".hermes")


def test_install_sh_prefers_avunel_dir_when_both_exist(tmp_path):
    """Both dirs on disk, no env: the Avunel default wins."""
    (tmp_path / ".hermes").mkdir()
    (tmp_path / ".avunel").mkdir()
    assert _install_sh_home(tmp_path) == str(tmp_path / ".avunel")


def test_install_sh_honors_hermes_home_alone(tmp_path):
    """A lone HERMES_HOME override is still respected (legacy compatibility)."""
    assert _install_sh_home(tmp_path, HERMES_HOME="/legacy") == "/legacy"


def test_install_sh_honors_avunel_home_alone(tmp_path):
    assert _install_sh_home(tmp_path, AVUNEL_HOME="/canonical") == "/canonical"


# ---------------------------------------------------------------------------
# setup-hermes.sh
# ---------------------------------------------------------------------------

def test_setup_sh_avunel_home_wins_over_hermes_home(tmp_path):
    assert _setup_sh_home(tmp_path, AVUNEL_HOME="/canonical", HERMES_HOME="/legacy") == "/canonical"


def test_setup_sh_fresh_install_defaults_to_avunel(tmp_path):
    assert _setup_sh_home(tmp_path) == str(tmp_path / ".avunel")


def test_setup_sh_reuses_lone_legacy_home(tmp_path):
    (tmp_path / ".hermes").mkdir()
    assert _setup_sh_home(tmp_path) == str(tmp_path / ".hermes")


def test_setup_sh_honors_hermes_home_alone(tmp_path):
    assert _setup_sh_home(tmp_path, HERMES_HOME="/legacy") == "/legacy"


# ---------------------------------------------------------------------------
# Python resolver (hermes_constants)
# ---------------------------------------------------------------------------

def test_python_resolver_avunel_home_wins(tmp_path, monkeypatch):
    from hermes_constants import get_process_hermes_home

    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.setenv("AVUNEL_HOME", str(tmp_path / "canonical"))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "legacy"))
    assert get_process_hermes_home() == tmp_path / "canonical"


def test_python_resolver_fresh_default_is_avunel(tmp_path, monkeypatch):
    from hermes_constants import get_process_hermes_home

    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.delenv("AVUNEL_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)
    assert get_process_hermes_home() == tmp_path / ".avunel"


def test_python_resolver_reuses_lone_legacy_home(tmp_path, monkeypatch):
    from hermes_constants import get_process_hermes_home

    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.delenv("AVUNEL_HOME", raising=False)
    monkeypatch.delenv("HERMES_HOME", raising=False)
    (tmp_path / ".hermes").mkdir()
    assert get_process_hermes_home() == tmp_path / ".hermes"


def test_python_resolver_hermes_home_alone(tmp_path, monkeypatch):
    from hermes_constants import get_process_hermes_home

    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    monkeypatch.delenv("AVUNEL_HOME", raising=False)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "legacy"))
    assert get_process_hermes_home() == tmp_path / "legacy"
