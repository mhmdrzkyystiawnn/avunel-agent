"""Release sampling follows tag chronology across CalVer and SemVer.

Two pickers choose release baselines: ``scripts/releases/pick_tags.py`` on the
release-mode path and ``scripts/sandbox/pick-release-tags.sh`` on the ordinary
pull-request path. They must agree on what counts as a release tag, or the E2E
matrix fails on a repository whose only release is a SemVer one.
"""
import json
import os
import subprocess

import pytest

from scripts.releases.pick_tags import pick_tags

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SHELL_PICKER = os.path.join(REPO_ROOT, "scripts", "sandbox", "pick-release-tags.sh")


def test_sampling_includes_newest_stable_and_excludes_candidate(tmp_path):
    def git(*args, env=None):
        return subprocess.run(["git", *args], cwd=tmp_path, env=env, check=True, capture_output=True, text=True)

    git("init", "-b", "main")
    git("config", "user.name", "fixture")
    git("config", "user.email", "fixture@example.invalid")
    for index, tag in enumerate(["v2026.7.20", "v0.27.0", "v0.28.0"]):
        env = {**os.environ, "GIT_AUTHOR_DATE": f"2026-08-{index + 1:02d}T00:00:00Z", "GIT_COMMITTER_DATE": f"2026-08-{index + 1:02d}T00:00:00Z"}
        git("commit", "--allow-empty", "-m", tag, env=env)
        git("tag", tag)
    assert pick_tags(tmp_path, 1, "v0.28.0") == ["v0.27.0"]
    assert pick_tags(tmp_path, 3, "v0.28.0") == ["v2026.7.20", "v0.27.0"]


def _shell_pick(tmp_path, tags, count=1):
    """Run the shell picker against a throwaway repo carrying ``tags``."""
    def git(*args):
        return subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True)

    git("init", "-b", "main")
    git("config", "user.name", "fixture")
    git("config", "user.email", "fixture@example.invalid")
    git("commit", "--allow-empty", "-m", "base")
    for tag in tags:
        git("tag", tag)
    result = subprocess.run(["bash", SHELL_PICKER, "--count", str(count), "--repo", str(tmp_path)],
                            capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


@pytest.mark.parametrize("tag", ["v0.1.0", "v2026.4.8", "v1.2.3.4"])
def test_shell_picker_accepts_every_release_tag_shape_the_python_picker_does(tmp_path, tag):
    """Both pickers must treat the same tag as a release.

    A four-digit-only major in the shell pattern made it reject ``v0.1.0`` while
    the Python picker accepted it, so the E2E matrix found no baselines on a
    repository whose only release was SemVer.
    """
    code, out, err = _shell_pick(tmp_path, [tag])
    assert code == 0, err
    assert json.loads(out) == [tag]
    # The Python picker agrees on the same tag.
    assert pick_tags(tmp_path, 1) == [tag]


@pytest.mark.parametrize("tag", ["v0.21.4+canary.20261010T130002Z", "backup/2026-01-01", "v1.2", "release-1"])
def test_shell_picker_still_rejects_non_release_tags(tmp_path, tag):
    """Widening the major to any digit count must not admit canaries or junk."""
    code, out, err = _shell_pick(tmp_path, [tag])
    assert code != 0
    assert "no release tags found" in err


def test_shell_picker_reports_absence_rather_than_an_empty_matrix(tmp_path):
    code, out, err = _shell_pick(tmp_path, ["v0.1.0+canary.20260101T000000Z"])
    assert code == 1
    assert out == ""
    assert "no release tags found" in err
