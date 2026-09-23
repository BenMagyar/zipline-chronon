import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from ai.chronon.cli.logger import get_logger

logger = get_logger()

# CI/automation escape hatch. Must be set at the environment level (pipeline
# YAML, repo secrets, etc.) — cannot be injected via a command-line flag.
_ALLOW_DIRTY_ENV_VAR = "ZIPLINE_ALLOW_DIRTY"

# Variables that, when inherited, override cwd= and redirect git to a different
# repository. Strip them so our repo_path scoping is never silently bypassed.
_GIT_OVERRIDE_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE")


def _clean_git_env() -> dict:
    """Return os.environ without git repository-override variables."""
    env = os.environ.copy()
    for var in _GIT_OVERRIDE_VARS:
        env.pop(var, None)
    return env


def get_git_user_email() -> str:
    """Get the user email from git config, falling back to USER env var."""
    try:
        result = subprocess.run(
            ["git", "config", "user.email"],
            capture_output=True,
            text=True,
            check=True,
        )
        email = result.stdout.strip()
        if email:
            return email
    except Exception:
        pass
    return os.environ.get("USER", "")


def get_current_branch(repo_path: Optional[str] = None) -> str:
    cwd = repo_path or None
    env = _clean_git_env()
    try:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, cwd=cwd, env=env)

        return (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=cwd, env=env)
            .decode("utf-8")
            .strip()
        )

    except subprocess.CalledProcessError as e:
        try:
            base = Path(repo_path).resolve() if repo_path else Path.cwd()
            head_file = (base / ".git" / "HEAD").resolve()

            if head_file.exists():
                content = head_file.read_text().strip()

                if content.startswith("ref: refs/heads/"):
                    return content.split("/")[-1]

        except Exception:
            pass

        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else 'Not a git repository or no commits'}",
            file=sys.stderr,
        )

        raise


def get_fork_point(base_branch: str = "main") -> str:
    try:
        return (
            subprocess.check_output(["git", "merge-base", base_branch, "HEAD"])
            .decode("utf-8")
            .strip()
        )

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else f'Could not determine fork point from {base_branch}'}",
            file=sys.stderr,
        )
        raise


def get_file_content_at_commit(file_path: str, commit: str) -> Optional[str]:
    try:
        return subprocess.check_output(["git", "show", f"{commit}:{file_path}"]).decode("utf-8")
    except subprocess.CalledProcessError:
        return None


def get_current_file_content(file_path: str) -> Optional[str]:
    try:
        return Path(file_path).read_text()
    except Exception:
        return None


def get_changes_since_commit(path: str, commit: Optional[str] = None) -> List[str]:
    path = Path(path).resolve()
    if not path.exists():
        print(f"⛔ Error: Path does not exist: {path}", file=sys.stderr)
        raise ValueError(f"Path does not exist: {path}")

    try:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        commit_range = f"{commit}..HEAD" if commit else "HEAD"

        changes = (
            subprocess.check_output(["git", "diff", "--name-only", commit_range, "--", str(path)])
            .decode("utf-8")
            .splitlines()
        )

    except subprocess.CalledProcessError:
        changes = (
            subprocess.check_output(["git", "diff", "--name-only", "--", str(path)])
            .decode("utf-8")
            .splitlines()
        )

    try:
        untracked = (
            subprocess.check_output(
                ["git", "ls-files", "--others", "--exclude-standard", str(path)]
            )
            .decode("utf-8")
            .splitlines()
        )

        changes.extend(untracked)

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else 'Failed to get untracked files'}",
            file=sys.stderr,
        )

        raise

    logger.info(f"Changes since commit: {changes}")

    return [change for change in changes if change.strip()]


def get_changes_since_fork(path: str, base_branch: str = "main") -> List[str]:
    try:
        fork_point = get_fork_point(base_branch)
        path = Path(path).resolve()

        # Get all potential changes
        changed_files = set(get_changes_since_commit(str(path), fork_point))

        # Filter out files that are identical to fork point
        real_changes = []
        for file in changed_files:
            fork_content = get_file_content_at_commit(file, fork_point)
            current_content = get_current_file_content(file)

            if fork_content != current_content:
                real_changes.append(file)

        logger.info(f"Changes since fork: {real_changes}")

        return real_changes

    except subprocess.CalledProcessError as e:
        print(
            f"⛔ Error: {e.stderr.decode('utf-8') if e.stderr else f'Failed to get changes since fork from {base_branch}'}",
            file=sys.stderr,
        )
        raise


def _get_dirty_files(repo_path: Optional[str] = None) -> List[str]:
    """Return lines from `git status --porcelain` for tracked modified/staged files.

    Scoped to repo_path when provided so unrelated workspace changes don't
    block unrelated commands.
    """
    cmd = ["git", "status", "--porcelain"]
    if repo_path:
        path = Path(repo_path).resolve()
        cmd += ["--", str(path)]
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, cwd=repo_path or None, env=_clean_git_env()).decode("utf-8")
        # Exclude untracked files (lines starting with '??') — only warn on
        # changes that are tracked but not committed.
        return [line for line in output.splitlines() if line and not line.startswith("??")]
    except subprocess.CalledProcessError:
        return []


def _get_unpushed_commits(repo_path: Optional[str] = None) -> Optional[List[str]]:
    """Return one-line summaries of local commits not present on origin.

    Returns None when upstream state cannot be verified — detached HEAD,
    branch never pushed to origin, or any git error. Callers treat None
    as "unverified / assume unpushed".

    Uses origin/<branch> explicitly rather than @{u} so the check works
    even when no tracking branch has been configured with git push -u.
    """
    cwd = repo_path or None
    env = _clean_git_env()
    # Resolve the current branch name. Returns None on detached HEAD or error.
    try:
        branch = (
            subprocess.check_output(
                ["git", "symbolic-ref", "--short", "HEAD"],
                stderr=subprocess.PIPE,
                cwd=cwd,
                env=env,
            )
            .decode("utf-8")
            .strip()
        )
        if not branch:
            return None
    except subprocess.CalledProcessError:
        # Detached HEAD or not in a git repo.
        return None

    # Compare local branch against its origin counterpart.
    try:
        output = (
            subprocess.check_output(
                ["git", "log", f"origin/{branch}..HEAD", "--oneline"],
                stderr=subprocess.PIPE,
                cwd=cwd,
                env=env,
            )
            .decode("utf-8")
            .splitlines()
        )
        return [line for line in output if line.strip()]
    except subprocess.CalledProcessError:
        # origin/<branch> doesn't exist (branch never pushed) or any other
        # git error — treat as unverified.
        return None


def assert_git_clean(repo_path: Optional[str] = None) -> None:
    """Abort if the working tree has uncommitted changes or unpushed commits.

    Decision tree:
      1. Clean → proceed silently.
      2. ZIPLINE_ALLOW_DIRTY=1 in env → warn to stderr and proceed (CI escape hatch).
      3. stdin is a TTY → require the user to type the current branch name to confirm.
      4. stdin is NOT a TTY (agent, pipe, subprocess) → hard error with no bypass.

    Step 4 is what makes this agent-proof: a subprocess caller has no TTY and
    cannot satisfy the interactive prompt regardless of what flags it passes.
    """
    import click

    # Check if we're even in a git repo; skip gracefully if not.
    try:
        subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, cwd=repo_path or None, env=_clean_git_env())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return

    dirty_files = _get_dirty_files(repo_path)
    unpushed = _get_unpushed_commits(repo_path)

    is_dirty = bool(dirty_files)
    no_upstream = unpushed is None
    is_unpushed = no_upstream or len(unpushed) > 0

    if not is_dirty and not is_unpushed:
        return

    # Build the problem description for messages / prompts.
    problems: List[str] = []
    if is_dirty:
        file_list = "\n  ".join(dirty_files[:20])
        suffix = f"\n  ... and {len(dirty_files) - 20} more" if len(dirty_files) > 20 else ""
        problems.append(
            f"Uncommitted changes in tracked files:\n  {file_list}{suffix}"
        )
    if no_upstream:
        problems.append(
            "No remote tracking branch is configured — cannot verify your commits are pushed.\n"
            "Fix: git push -u origin <branch>"
        )
    elif unpushed:
        commit_list = "\n  ".join(unpushed[:10])
        suffix = f"\n  ... and {len(unpushed) - 10} more" if len(unpushed) > 10 else ""
        problems.append(
            f"{len(unpushed)} local commit(s) not pushed to origin:\n  {commit_list}{suffix}"
        )

    problem_text = "\n\n".join(problems)

    # --- CI escape hatch (env var, not a CLI flag) ---
    if os.environ.get(_ALLOW_DIRTY_ENV_VAR, "").strip().lower() in ("1", "true", "yes"):
        print(
            f"⚠️  ZIPLINE_ALLOW_DIRTY is set — proceeding despite dirty git state:\n{problem_text}",
            file=sys.stderr,
        )
        return

    # --- Interactive path: require the user to type the branch name ---
    if sys.stdin.isatty():
        try:
            current_branch = get_current_branch(repo_path)
        except Exception:
            current_branch = "<unknown>"

        click.echo(
            click.style(
                f"\n⚠️  Your git working tree is not clean:\n\n{problem_text}\n",
                fg="yellow",
            ),
            err=True,
        )
        # Give a more specific nudge when the only issue is a missing upstream.
        if no_upstream and not is_dirty:
            click.echo(
                "Run 'git push -u origin <branch>' to publish your branch, then retry.\n",
                err=True,
            )
        else:
            click.echo(
                "Hub commands upload your local compiled confs to the backend. "
                "Running with dirty or un-pushed changes means the cluster may\n"
                "execute confs that cannot be reproduced from your remote branch.\n",
                err=True,
            )
        typed = click.prompt(
            click.style(
                f"Type the branch name '{current_branch}' to proceed anyway, or Ctrl-C to abort",
                fg="yellow",
            ),
            default="",
            show_default=False,
        )
        if typed.strip() != current_branch:
            raise click.Abort()
        return

    # --- Non-TTY path: hard error, no bypass ---
    raise click.UsageError(
        f"Git working tree is not clean. Hub commands require a clean, pushed branch "
        f"so the cluster can reproduce your confs from the remote.\n\n"
        f"{problem_text}\n\n"
        f"Fix: commit and push your changes, then re-run the command.\n"
        f"CI bypass: set {_ALLOW_DIRTY_ENV_VAR}=1 in your pipeline environment."
    )
