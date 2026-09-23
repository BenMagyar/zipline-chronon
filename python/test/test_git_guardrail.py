"""Tests for the git-clean guardrail in assert_git_clean."""
import subprocess
from unittest.mock import patch

import click
import pytest

from ai.chronon.cli.git_utils import (
    _ALLOW_DIRTY_ENV_VAR,
    _get_dirty_files,
    _get_unpushed_commits,
    assert_git_clean,
)


# ---------------------------------------------------------------------------
# _get_dirty_files
# ---------------------------------------------------------------------------

class TestGetDirtyFiles:
    def test_returns_empty_on_clean(self):
        with patch("subprocess.check_output", return_value=b""):
            assert _get_dirty_files() == []

    def test_returns_modified_and_staged_lines(self):
        output = b" M group_bys/team/x.py\nA  group_bys/team/y.py\n"
        with patch("subprocess.check_output", return_value=output):
            result = _get_dirty_files()
        assert " M group_bys/team/x.py" in result
        assert "A  group_bys/team/y.py" in result

    def test_excludes_untracked_files(self):
        output = b"?? group_bys/team/new.py\n M group_bys/team/old.py\n"
        with patch("subprocess.check_output", return_value=output):
            result = _get_dirty_files()
        assert all(not line.startswith("??") for line in result)
        assert len(result) == 1

    def test_returns_empty_on_subprocess_error(self):
        with patch("subprocess.check_output", side_effect=subprocess.CalledProcessError(1, "git")):
            assert _get_dirty_files() == []

    def test_passes_repo_path_to_git(self):
        with patch("subprocess.check_output", return_value=b"") as mock_check:
            _get_dirty_files("/some/repo")
        cmd = mock_check.call_args[0][0]
        assert "--" in cmd
        assert any("/some/repo" in str(part) for part in cmd)


# ---------------------------------------------------------------------------
# _get_unpushed_commits
# ---------------------------------------------------------------------------

class TestGetUnpushedCommits:
    def test_returns_empty_list_when_clean(self):
        with patch("subprocess.check_output", side_effect=[b"main\n", b""]):
            assert _get_unpushed_commits() == []

    def test_returns_commit_lines(self):
        # symbolic-ref succeeds first, then log returns commits
        responses = [b"main\n", b"abc1234 feat: add feature\ndef5678 fix: small fix\n"]
        with patch("subprocess.check_output", side_effect=responses):
            result = _get_unpushed_commits()
        assert len(result) == 2
        assert "abc1234 feat: add feature" in result

    def test_returns_none_when_branch_not_on_origin(self):
        # symbolic-ref succeeds; origin/<branch> doesn't exist → CalledProcessError
        sym_ref_ok = b"feature/foo\n"
        log_err = subprocess.CalledProcessError(128, "git", stderr=b"unknown revision")
        with patch("subprocess.check_output", side_effect=[sym_ref_ok, log_err]):
            assert _get_unpushed_commits() is None

    def test_returns_none_on_detached_head(self):
        exc = subprocess.CalledProcessError(128, "git", stderr=b"fatal: ref HEAD is not a symbolic ref")
        with patch("subprocess.check_output", side_effect=exc):
            assert _get_unpushed_commits() is None

    def test_returns_none_on_any_git_error(self):
        sym_ref_ok = b"main\n"
        log_err = subprocess.CalledProcessError(1, "git", stderr=b"some other error")
        with patch("subprocess.check_output", side_effect=[sym_ref_ok, log_err]):
            assert _get_unpushed_commits() is None


# ---------------------------------------------------------------------------
# assert_git_clean — full decision tree
# ---------------------------------------------------------------------------

class TestAssertGitClean:

    # 1. Clean repo → no exception
    def test_passes_when_clean(self):
        with patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]), \
             patch("subprocess.check_output", return_value=b"abc123"):
            assert_git_clean()  # must not raise

    # 2. ZIPLINE_ALLOW_DIRTY=1 → warn and proceed even when dirty
    def test_allow_dirty_env_var_bypasses_check(self, monkeypatch, capsys):
        monkeypatch.setenv(_ALLOW_DIRTY_ENV_VAR, "1")
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]):
            assert_git_clean()  # must not raise
        captured = capsys.readouterr()
        assert "ZIPLINE_ALLOW_DIRTY" in captured.err

    def test_allow_dirty_env_var_true_string(self, monkeypatch):
        monkeypatch.setenv(_ALLOW_DIRTY_ENV_VAR, "true")
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]):
            assert_git_clean()  # must not raise

    # 3. Non-TTY + dirty → hard UsageError (agent-proof path)
    def test_non_tty_dirty_files_raises_usage_error(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M group_bys/team/x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            with pytest.raises(click.UsageError) as exc_info:
                assert_git_clean()
        assert "group_bys/team/x.py" in str(exc_info.value)
        assert _ALLOW_DIRTY_ENV_VAR in str(exc_info.value)

    def test_non_tty_unpushed_commits_raises_usage_error(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=["abc1234 feat: thing"]), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            with pytest.raises(click.UsageError) as exc_info:
                assert_git_clean()
        assert "1 local commit" in str(exc_info.value)

    def test_non_tty_no_upstream_raises_usage_error(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=None), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            with pytest.raises(click.UsageError) as exc_info:
                assert_git_clean()
        assert "No remote tracking branch" in str(exc_info.value)

    # 4. TTY + correct branch name typed → proceed
    def test_tty_correct_branch_name_proceeds(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]), \
             patch("ai.chronon.cli.git_utils.get_current_branch", return_value="feature/my-branch"), \
             patch("sys.stdin") as mock_stdin, \
             patch("click.prompt", return_value="feature/my-branch"):
            mock_stdin.isatty.return_value = True
            assert_git_clean()  # must not raise

    # 5. TTY + wrong branch name typed → Abort
    def test_tty_wrong_branch_name_aborts(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]), \
             patch("ai.chronon.cli.git_utils.get_current_branch", return_value="feature/my-branch"), \
             patch("sys.stdin") as mock_stdin, \
             patch("click.prompt", return_value="main"):
            mock_stdin.isatty.return_value = True
            with pytest.raises(click.Abort):
                assert_git_clean()

    # 6. Not in a git repo → skip gracefully
    def test_skips_gracefully_outside_git_repo(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", side_effect=subprocess.CalledProcessError(128, "git")):
            assert_git_clean()  # must not raise

    def test_skips_gracefully_when_git_not_installed(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", side_effect=FileNotFoundError):
            assert_git_clean()  # must not raise

    # 7. repo_path is forwarded to _get_dirty_files
    def test_repo_path_forwarded(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[]) as mock_dirty, \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]):
            assert_git_clean("/my/repo")
        mock_dirty.assert_called_once_with("/my/repo")

    # 8. Both dirty files AND unpushed commits present → both appear in the error
    def test_non_tty_both_conditions_in_error(self, monkeypatch):
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M group_bys/team/x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=["abc1234 feat: thing"]), \
             patch("sys.stdin") as mock_stdin:
            mock_stdin.isatty.return_value = False
            with pytest.raises(click.UsageError) as exc_info:
                assert_git_clean()
        msg = str(exc_info.value)
        assert "group_bys/team/x.py" in msg
        assert "1 local commit" in msg

    # 9. Env var is case-insensitive — "TRUE" and "Yes" should also bypass
    def test_allow_dirty_env_var_case_insensitive(self, monkeypatch, capsys):
        for value in ("TRUE", "Yes", "YES"):
            monkeypatch.setenv(_ALLOW_DIRTY_ENV_VAR, value)
            with patch("subprocess.check_output", return_value=b"abc123"), \
                 patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
                 patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]):
                assert_git_clean()  # must not raise

    # 10. Guardrail runs before hub_uploader.compute_and_upload_diffs on the dirty path
    def test_non_tty_guardrail_fires_before_upload(self, monkeypatch):
        """compute_and_upload_diffs must never be called when the guardrail blocks."""
        monkeypatch.delenv(_ALLOW_DIRTY_ENV_VAR, raising=False)
        with patch("subprocess.check_output", return_value=b"abc123"), \
             patch("ai.chronon.cli.git_utils._get_dirty_files", return_value=[" M x.py"]), \
             patch("ai.chronon.cli.git_utils._get_unpushed_commits", return_value=[]), \
             patch("sys.stdin") as mock_stdin, \
             patch("ai.chronon.repo.hub_uploader.compute_and_upload_diffs") as mock_upload:
            mock_stdin.isatty.return_value = False
            with pytest.raises(click.UsageError):
                from ai.chronon.cli.git_utils import assert_git_clean as _check
                _check()
        mock_upload.assert_not_called()
