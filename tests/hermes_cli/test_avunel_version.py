"""Avunel version identity: independent product version + live Git provenance.

These tests build THROWAWAY git repositories (never the real checkout) so the
upstream/local/carried/dirty values are deterministic and prove the values are
computed from Git — not hardcoded. The brief's example numbers (27c02f63,
1982d544, +11) must never appear as constants; every assertion here derives its
expectation from the fixture it just built.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from hermes_cli import version_info
from hermes_constants import AVUNEL_VERSION


def _git(repo: Path, *args: str, env: dict | None = None) -> str:
    result = subprocess.run(
        ["git", *args], cwd=str(repo), capture_output=True, text=True,
        env={**_git_env(), **(env or {})},
    )
    assert result.returncode == 0, f"git {' '.join(args)} failed: {result.stderr}"
    return result.stdout.strip()


def _git_env() -> dict:
    import os
    return {
        **os.environ,
        "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
    }


def _commit(repo: Path, message: str, *, ts: int = 1_700_000_000) -> str:
    (repo / "f.txt").write_text(message, encoding="utf-8")
    _git(repo, "add", "f.txt")
    env = {"GIT_AUTHOR_DATE": f"{ts} +0000", "GIT_COMMITTER_DATE": f"{ts} +0000"}
    _git(repo, "commit", "-m", message, env=env)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def fork(tmp_path, monkeypatch):
    """An 'upstream' repo + a 'fork' with N commits carried on top of it.

    Layout mirrors the real fork: `origin/main` = the upstream base (tagged with
    a release version), HEAD = the fork tip. Returns (fork_dir, upstream_sha,
    local_sha, carried_count).
    """
    upstream = tmp_path / "upstream"
    upstream.mkdir()
    _git(upstream, "init", "-q", "-b", "main")
    base = _commit(upstream, "base", ts=1_700_000_000)
    _git(upstream, "tag", "v0.21.5", base)  # a release tag the fork inherits

    fork_dir = tmp_path / "fork"
    fork_dir.mkdir()
    _git(fork_dir, "init", "-q", "-b", "temporal-memory")
    _git(fork_dir, "remote", "add", "origin", str(upstream))
    _git(fork_dir, "fetch", "-q", "origin", "--tags")
    _git(fork_dir, "reset", "-q", "--hard", "origin/main")
    carried = 3
    local = base
    for i in range(carried):
        local = _commit(fork_dir, f"fork {i}", ts=1_700_000_100 + i)

    monkeypatch.setattr(version_info, "_resolve_repo_dir", lambda: fork_dir)
    version_info._reset_version_info_cache()
    yield fork_dir, base[:8], local[:8], carried
    version_info._reset_version_info_cache()


def test_avunel_version_is_independent_of_hermes(fork):
    """Avunel reports its own version; the Hermes version is a separate field."""
    p = version_info.get_avunel_provenance()
    assert p.avunel_version == AVUNEL_VERSION == "0.1.0"
    # The Hermes side is whatever the checkout says — never Avunel's number.
    assert p.hermes_version != AVUNEL_VERSION


def test_provenance_is_computed_from_git(fork):
    fork_dir, base_sha, local_sha, carried = fork
    p = version_info.get_avunel_provenance()
    assert p.upstream_sha == base_sha
    assert p.local_sha == local_sha
    assert p.carried_commits == carried
    # Guard against a regression that copies the example constants in.
    assert p.local_sha not in {"1982d544"}
    assert p.upstream_sha not in {"27c02f63"}


def test_carried_count_is_ancestry_not_total_commits(fork):
    """carried = commits reachable from HEAD but not upstream (rev-list base..HEAD)."""
    fork_dir, _base, _local, carried = fork
    p = version_info.get_avunel_provenance()
    total = int(_git(fork_dir, "rev-list", "--count", "HEAD"))
    # The fork has base + carried commits total; carried must be the smaller delta.
    assert p.carried_commits == carried
    assert p.carried_commits == total - 1


def test_dirty_reflects_uncommitted_changes(fork):
    fork_dir, *_ = fork
    assert version_info.get_avunel_provenance().dirty is False
    (fork_dir / "f.txt").write_text("uncommitted", encoding="utf-8")
    version_info._reset_version_info_cache()
    assert version_info.get_avunel_provenance().dirty is True


def test_missing_upstream_reports_unknown_not_zero(tmp_path, monkeypatch):
    """No resolvable origin → Origin: unknown, and carried is None (never a fake 0)."""
    repo = tmp_path / "solo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _commit(repo, "only")
    monkeypatch.setattr(version_info, "_resolve_repo_dir", lambda: repo)
    version_info._reset_version_info_cache()

    p = version_info.get_avunel_provenance()
    assert p.upstream_sha is None
    assert p.carried_commits is None
    lines = version_info.format_avunel_version_lines()
    assert "Origin: unknown" in lines
    assert not any("commits" in line for line in lines)
    version_info._reset_version_info_cache()


def test_git_unavailable_degrades_safely(fork, monkeypatch):
    """Git failing everywhere → provenance fields are None, version still shows."""
    monkeypatch.setattr(version_info, "_run_git", lambda *a, **k: None)
    monkeypatch.setattr(version_info, "_is_dirty", lambda *a, **k: False)
    p = version_info.get_avunel_provenance()
    assert p.upstream_sha is None and p.local_sha is None and p.carried_commits is None
    assert p.avunel_version == "0.1.0"
    lines = version_info.format_avunel_version_lines()
    assert lines[0] == "Avunel 0.1.0"


def test_display_block_shape(fork):
    lines = version_info.format_avunel_version_lines()
    assert lines[0] == "Avunel 0.1.0"
    assert any(l.startswith("Base: Hermes v") for l in lines)
    assert any(l.startswith("Origin: ") for l in lines)
    assert any(l.startswith("Local: ") and "commits" in l for l in lines)


def test_carried_singular_wording(fork):
    """Carried count follows Git exactly; the singular case reads '(+1 commit)'."""
    fork_dir, base_sha, _local, _carried = fork
    # Collapse the fork to exactly one commit on top of the upstream base.
    _git(fork_dir, "reset", "-q", "--hard", base_sha)
    one = _commit(fork_dir, "solo", ts=1_700_000_600)
    version_info._reset_version_info_cache()
    lines = version_info.format_avunel_version_lines()
    assert f"Local: {one[:8]} (+1 commit)" in lines


def test_date_uses_head_commit_date_not_wall_clock(fork):
    """The date comes from the HEAD commit timestamp, not 'now'."""
    fork_dir, *_ = fork
    head_ts = int(_git(fork_dir, "log", "-1", "--format=%ct", "HEAD"))
    p = version_info.get_avunel_provenance()
    assert p.commit_date == head_ts
    # Fixture commits are stamped in Nov 2023 — never today's date.
    lines = version_info.format_avunel_version_lines()
    assert any("2023.11." in l for l in lines)
