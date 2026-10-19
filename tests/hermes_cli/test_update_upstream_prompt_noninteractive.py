"""Avunel never offers an ``upstream`` remote, interactive or not.

Avunel does not track upstream Hermes (K9). ``_sync_with_upstream_if_needed``
and ``_offer_upstream_remote`` are stubs that return False without prompting,
without touching git remotes, and without persisting a skip marker.

These tests pin that stub: no prompt is ever issued, in any stdio combination
or with any ``input_fn``, and no remote is ever added. The historical concern
(#60240: a bare ``input()`` blocking unattended updates) is moot because no
prompt exists any more — the assertion below is stronger than the gate it
replaced.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from hermes_cli import update_cmd


@pytest.fixture
def fork_without_upstream(tmp_path):
    with patch.object(
        update_cmd, "_has_upstream_remote", return_value=False
    ), patch.object(
        update_cmd, "_should_skip_upstream_prompt", return_value=False
    ), patch.object(
        update_cmd, "_add_upstream_remote", return_value=True
    ) as add_remote, patch.object(
        update_cmd, "_mark_skip_upstream_prompt"
    ) as mark_skip, patch("builtins.input") as stdin_input:
        yield SimpleNamespace(
            cwd=tmp_path,
            add_remote=add_remote,
            mark_skip=mark_skip,
            stdin_input=stdin_input,
        )


def _tty(stdin: bool, stdout: bool):
    return (
        patch.object(update_cmd.sys.stdin, "isatty", return_value=stdin),
        patch.object(update_cmd.sys.stdout, "isatty", return_value=stdout),
    )


class TestNoUpstreamPrompt:
    def test_assume_yes_skips_prompt_without_touching_remotes(
        self, fork_without_upstream, capsys
    ):
        p_in, p_out = _tty(True, True)
        with p_in, p_out:
            checked = update_cmd._sync_with_upstream_if_needed(
                ["git"], fork_without_upstream.cwd, assume_yes=True
            )

        assert checked is False
        fork_without_upstream.stdin_input.assert_not_called()
        fork_without_upstream.add_remote.assert_not_called()
        fork_without_upstream.mark_skip.assert_not_called()

    @pytest.mark.parametrize(
        "stdin_tty,stdout_tty", [(False, False), (False, True), (True, False)]
    )
    def test_non_tty_skips_prompt_without_persisting_decline(
        self, fork_without_upstream, capsys, stdin_tty, stdout_tty
    ):
        p_in, p_out = _tty(stdin_tty, stdout_tty)
        with p_in, p_out:
            checked = update_cmd._sync_with_upstream_if_needed(
                ["git"], fork_without_upstream.cwd
            )

        assert checked is False
        fork_without_upstream.stdin_input.assert_not_called()
        fork_without_upstream.add_remote.assert_not_called()
        fork_without_upstream.mark_skip.assert_not_called()

    @pytest.mark.parametrize("assume_yes", [True, False])
    def test_input_fn_is_never_invoked(self, fork_without_upstream, assume_yes):
        """A gateway-provided ``input_fn`` is never reached: there is nothing to ask."""
        prompts = []

        def gw_input(prompt, default=""):
            prompts.append((prompt, default))
            return "y"

        p_in, p_out = _tty(True, True)
        with p_in, p_out:
            checked = update_cmd._sync_with_upstream_if_needed(
                ["git"], fork_without_upstream.cwd, assume_yes=assume_yes, input_fn=gw_input
            )

        assert checked is False
        assert prompts == []
        fork_without_upstream.stdin_input.assert_not_called()
        fork_without_upstream.add_remote.assert_not_called()
        fork_without_upstream.mark_skip.assert_not_called()

    def test_offer_upstream_remote_is_a_no_op_stub(self, fork_without_upstream):
        """Called directly, the offer stub still declines without prompting."""
        from hermes_cli.update_cmd_git import _offer_upstream_remote

        p_in, p_out = _tty(True, True)
        with p_in, p_out:
            added = _offer_upstream_remote(
                ["git"], fork_without_upstream.cwd, assume_yes=False, input_fn=None
            )

        assert added is False
        fork_without_upstream.stdin_input.assert_not_called()
        fork_without_upstream.add_remote.assert_not_called()
