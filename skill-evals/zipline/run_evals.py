#!/usr/bin/python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HARNESS_DIR = Path(__file__).resolve().parent
CASES_DIR = HARNESS_DIR / "cases"
DEFAULT_RUNS_DIR = HARNESS_DIR / "runs"
SKILL_SOURCE_RELATIVE = Path("python/src/ai/chronon/resources/agent_context.md")
INSTALLED_SKILL = Path.home() / ".codex" / "skills" / "zipline" / "SKILL.md"
REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
SANDBOX_MODES = {"read-only", "workspace-write", "danger-full-access"}
BRANCH_CREATE_PATTERN = re.compile(
    r"\bgit(?:\s+-C\s+\S+)?\s+(?:"
    r"switch\s+(?:-c|--create)(?:\s+|=)|"
    r"checkout\s+(?:-b|-B)(?:\s+|=)"
    r")",
    re.IGNORECASE,
)


@dataclass
class RunResult:
    code: int
    signal: str | None
    timed_out: bool
    stdout: bytes
    stderr: bytes


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Zipline skill evaluations in isolated Git checkouts."
    )
    parser.add_argument("--model", required=True, help="Codex model recorded for every run")
    parser.add_argument("--reasoning-effort", choices=sorted(REASONING_EFFORTS))
    parser.add_argument("--case", dest="cases", action="append", default=[])
    parser.add_argument("--tag")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--base-ref")
    parser.add_argument("--sandbox", choices=sorted(SANDBOX_MODES), default="workspace-write")
    parser.add_argument("--no-approve-for-me", dest="approve_for_me", action="store_false")
    parser.set_defaults(approve_for_me=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_RUNS_DIR)
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--codex-arg", dest="codex_args", action="append", default=[])
    parser.add_argument("--keep-worktrees", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    options = parser.parse_args(argv)

    if options.repeat < 1:
        parser.error("--repeat must be a positive integer")
    if options.timeout_seconds < 1:
        parser.error("--timeout-seconds must be a positive integer")
    if options.approve_for_me and options.sandbox != "workspace-write":
        parser.error("automatic approval requires the workspace-write sandbox")
    options.output_dir = options.output_dir.expanduser().resolve()
    if os.sep in options.codex_bin or (os.altsep and os.altsep in options.codex_bin):
        options.codex_bin = str(Path(options.codex_bin).expanduser().resolve())
    return options


def run(
    command: str,
    args: list[str],
    *,
    cwd: Path | str | None = None,
    env: dict[str, str] | None = None,
    input_data: str | bytes | None = None,
    timeout_seconds: float | None = None,
) -> RunResult:
    payload = input_data.encode() if isinstance(input_data, str) else input_data
    try:
        process = subprocess.Popen(
            [command, *args],
            cwd=cwd,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as error:
        raise RuntimeError(f"Unable to start {command}: {error}") from error

    timed_out = False
    try:
        stdout, stderr = process.communicate(input=payload, timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            stdout, stderr = process.communicate()

    return_code = process.returncode if process.returncode is not None else 1
    signal_name = None
    if return_code < 0:
        try:
            signal_name = signal.Signals(-return_code).name
        except ValueError:
            signal_name = f"SIG{-return_code}"
        return_code = 1
    return RunResult(return_code, signal_name, timed_out, stdout, stderr)


def run_checked(
    command: str,
    args: list[str],
    *,
    cwd: Path | str | None = None,
    env: dict[str, str] | None = None,
) -> RunResult:
    result = run(command, args, cwd=cwd, env=env)
    if result.code != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"{command} {' '.join(args)} failed ({result.code}){suffix}")
    return result


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def hash_file(path: Path, algorithm: str) -> str:
    digest = hashlib.new(algorithm)
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_optional_file(path: Path, algorithm: str = "sha256") -> str | None:
    return hash_file(path, algorithm) if path.exists() else None


def iso_for_file(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")


def safe_id(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", value)


def timestamp_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def format_exception(error: BaseException) -> str:
    return "".join(traceback.format_exception(type(error), error, error.__traceback__))


def validate_case(test_case: Any, file_path: Path) -> dict[str, Any]:
    def fail(message: str) -> None:
        raise ValueError(f"{file_path}: {message}")

    if not isinstance(test_case, dict):
        fail("case must be an object")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", str(test_case.get("id", ""))):
        fail("id must be kebab-case")
    if not isinstance(test_case.get("description"), str) or not test_case["description"].strip():
        fail("description is required")
    if not isinstance(test_case.get("prompt"), str) or not test_case["prompt"].strip():
        fail("prompt is required")
    if not isinstance(test_case.get("tags"), list):
        fail("tags must be an array")
    if "initial_branch" in test_case and (
        not isinstance(test_case["initial_branch"], str)
        or not test_case["initial_branch"].strip()
    ):
        fail("initial_branch must be a non-empty string")
    expectations = test_case.get("expectations")
    if not isinstance(expectations, dict):
        fail("expectations are required")
    if not isinstance(expectations.get("allow_file_changes"), bool):
        fail("expectations.allow_file_changes must be boolean")
    if "required_new_branch_before_file_change" in expectations and not isinstance(
        expectations["required_new_branch_before_file_change"], bool
    ):
        fail("expectations.required_new_branch_before_file_change must be boolean")
    criteria = expectations.get("judge_criteria")
    if not isinstance(criteria, list) or not criteria:
        fail("expectations.judge_criteria must be a non-empty array")
    for field in (
        "required_changed_globs",
        "forbidden_changed_globs",
        "required_command_patterns",
        "forbidden_diff_patterns",
        "required_response_patterns",
    ):
        if field in expectations and not isinstance(expectations[field], list):
            fail(f"expectations.{field} must be an array")
    return test_case


def load_cases() -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    seen: set[str] = set()
    for file_path in sorted(CASES_DIR.glob("*.json")):
        test_case = validate_case(json.loads(file_path.read_text(encoding="utf-8")), file_path)
        if test_case["id"] in seen:
            raise ValueError(f"Duplicate case id: {test_case['id']}")
        seen.add(test_case["id"])
        test_case["case_file"] = file_path
        cases.append(test_case)
    return cases


def resolve_repo_root() -> Path:
    result = run_checked("git", ["rev-parse", "--show-toplevel"], cwd=HARNESS_DIR)
    return Path(result.stdout.decode().strip())


def get_version(command: str, args: list[str] | None = None) -> dict[str, Any]:
    try:
        result = run(command, args or ["--version"])
        output = (result.stdout.decode(errors="replace") + "\n" + result.stderr.decode(errors="replace")).strip()
        return {"available": result.code == 0, "exit_code": result.code, "output": output}
    except Exception as error:
        return {"available": False, "exit_code": None, "output": str(error)}


def collect_skill_metadata(repo_root: Path) -> dict[str, Any]:
    source_path = repo_root / SKILL_SOURCE_RELATIVE
    if not source_path.exists():
        raise FileNotFoundError(f"Skill source not found: {source_path}")
    if not INSTALLED_SKILL.exists():
        raise FileNotFoundError(f"Installed Codex skill not found: {INSTALLED_SKILL}")

    blob = run_checked("git", ["hash-object", str(SKILL_SOURCE_RELATIVE)], cwd=repo_root)
    log = run("git", ["log", "-1", "--format=%H%x00%cI", "--", str(SKILL_SOURCE_RELATIVE)], cwd=repo_root)
    dirty = run_checked("git", ["status", "--porcelain", "--", str(SKILL_SOURCE_RELATIVE)], cwd=repo_root)
    log_values = log.stdout.decode(errors="replace").strip().split("\0")
    last_commit = log_values[0] if log_values and log_values[0] else None
    last_commit_date = log_values[1] if len(log_values) > 1 and log_values[1] else None
    source_md5 = hash_file(source_path, "md5")
    installed_md5 = hash_file(INSTALLED_SKILL, "md5")

    return {
        "source_path": str(source_path),
        "installed_path": str(INSTALLED_SKILL),
        "md5": source_md5,
        "sha256": hash_file(source_path, "sha256"),
        "installed_md5": installed_md5,
        "installed_sha256": hash_file(INSTALLED_SKILL, "sha256"),
        "installed_matches_source": source_md5 == installed_md5,
        "source_modified_at": iso_for_file(source_path),
        "installed_modified_at": iso_for_file(INSTALLED_SKILL),
        "git_blob": blob.stdout.decode().strip(),
        "last_commit": last_commit,
        "last_commit_date": last_commit_date,
        "source_dirty": bool(dirty.stdout.decode().strip()),
    }


def parse_porcelain_v1_z(data: bytes) -> list[dict[str, Any]]:
    tokens = [token for token in data.decode("utf-8", errors="replace").split("\0") if token]
    files: list[dict[str, Any]] = []
    index = 0
    while index < len(tokens):
        entry = tokens[index]
        index += 1
        if len(entry) < 4:
            continue
        code = entry[:2]
        record: dict[str, Any] = {
            "path": entry[3:],
            "status_code": code,
            "status": "modified",
        }
        if code == "??" or "A" in code:
            record["status"] = "created"
        elif "D" in code:
            record["status"] = "deleted"
        elif "R" in code:
            record["status"] = "renamed"
        elif "C" in code:
            record["status"] = "copied"
        if "R" in code or "C" in code:
            record["original_path"] = tokens[index] if index < len(tokens) else None
            index += 1
        files.append(record)
    return files


def parse_events(jsonl: str) -> dict[str, Any]:
    events: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    for line_number, line in enumerate(jsonl.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except Exception as error:
            parse_errors.append({"line": line_number, "error": str(error), "text": line[:500]})

    usage: dict[str, int] = {}
    command_by_id: dict[str, dict[str, Any]] = {}
    tool_calls: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    started_file_changes: set[str] = set()
    agent_messages: list[str] = []
    errors: list[dict[str, Any]] = []
    thread_id = None

    for event_index, event in enumerate(events):
        if event.get("type") == "thread.started":
            thread_id = event.get("thread_id") or thread_id
        if event.get("type") == "turn.completed" and isinstance(event.get("usage"), dict):
            for key, value in event["usage"].items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    usage[key] = usage.get(key, 0) + int(value)
        if event.get("type") in {"error", "turn.failed"}:
            errors.append(event)
        item = event.get("item")
        if not isinstance(item, dict):
            continue
        item_type = item.get("type")
        if item_type == "command_execution":
            key = item.get("id") or f"{item.get('command', 'command')}-{len(command_by_id)}"
            command_by_id[str(key)] = {
                "id": item.get("id"),
                "command": item.get("command"),
                "status": item.get("status"),
                "exit_code": item.get("exit_code"),
            }
            if event.get("type") == "item.completed":
                actions.append(
                    {
                        "event_index": event_index,
                        "type": "command_execution",
                        "id": item.get("id"),
                        "command": item.get("command"),
                        "status": item.get("status"),
                        "exit_code": item.get("exit_code"),
                    }
                )
        elif item_type == "agent_message" and isinstance(item.get("text"), str):
            agent_messages.append(item["text"])
        elif item_type in {"mcp_tool_call", "web_search", "file_change"}:
            tool_calls.append(
                {
                    "id": item.get("id"),
                    "type": item_type,
                    "name": item.get("name") or item.get("tool"),
                    "status": item.get("status"),
                }
            )
            if item_type == "file_change":
                action_id = str(item.get("id") or f"file-change-{len(actions)}")
                if action_id not in started_file_changes:
                    started_file_changes.add(action_id)
                    actions.append(
                        {
                            "event_index": event_index,
                            "type": "file_change",
                            "id": item.get("id"),
                            "status": item.get("status"),
                        }
                    )

    return {
        "event_count": len(events),
        "parse_errors": parse_errors,
        "thread_id": thread_id,
        "usage": usage,
        "commands": list(command_by_id.values()),
        "tool_calls": tool_calls,
        "actions": sorted(actions, key=lambda action: action["event_index"]),
        "errors": errors,
        "last_agent_message": agent_messages[-1] if agent_messages else "",
    }


def glob_to_regex(glob: str) -> re.Pattern[str]:
    expression = "^"
    index = 0
    while index < len(glob):
        character = glob[index]
        if character == "*":
            if index + 1 < len(glob) and glob[index + 1] == "*":
                expression += ".*"
                index += 2
                continue
            expression += "[^/]*"
        elif character == "?":
            expression += "[^/]"
        else:
            expression += re.escape(character)
        index += 1
    return re.compile(expression + "$")


def matches_glob(file_path: str, glob: str) -> bool:
    return bool(glob_to_regex(glob).search(file_path))


def skill_loaded(trace: dict[str, Any]) -> bool:
    return any(
        "/skills/zipline/SKILL.md" in str(command.get("command") or "")
        for command in trace["commands"]
    )


def branch_before_first_file_change(
    trace: dict[str, Any], initial_branch: str | None, final_branch: str | None
) -> tuple[bool, str]:
    branch_action = next(
        (
            action
            for action in trace["actions"]
            if action["type"] == "command_execution"
            and action.get("exit_code") == 0
            and BRANCH_CREATE_PATTERN.search(str(action.get("command") or ""))
        ),
        None,
    )
    first_file_change = next(
        (action for action in trace["actions"] if action["type"] == "file_change"),
        None,
    )
    passed = (
        bool(initial_branch)
        and bool(final_branch)
        and final_branch != initial_branch
        and branch_action is not None
        and first_file_change is not None
        and branch_action["event_index"] < first_file_change["event_index"]
    )
    branch_command = branch_action.get("command") if branch_action else "not observed"
    file_change_index = (
        first_file_change["event_index"] if first_file_change is not None else "not observed"
    )
    return (
        passed,
        f"initial={initial_branch or 'detached'}, final={final_branch or 'detached'}, "
        f"branch_command={branch_command}, first_file_change_event={file_change_index}",
    )


def evaluate_checks(
    test_case: dict[str, Any],
    files: list[dict[str, Any]],
    diff: str,
    response: str,
    trace: dict[str, Any],
    codex_exit_code: int,
    codex_timed_out: bool,
    skill_stable: bool,
    initial_branch: str | None,
    final_branch: str | None,
) -> dict[str, Any]:
    expectations = test_case["expectations"]
    checks: list[dict[str, Any]] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    loaded = skill_loaded(trace)
    add("Codex exited successfully", codex_exit_code == 0, f"Exit code: {codex_exit_code}")
    add(
        "Codex completed before timeout",
        not codex_timed_out,
        "Timed out" if codex_timed_out else "Completed in time",
    )
    add(
        "Codex trace completed without errors",
        not trace["errors"],
        "No trace errors" if not trace["errors"] else f"{len(trace['errors'])} trace error(s)",
    )
    add(
        "Codex event stream parsed",
        not trace["parse_errors"],
        "No JSONL parse errors"
        if not trace["parse_errors"]
        else f"{len(trace['parse_errors'])} parse error(s)",
    )
    add(
        "Skill remained stable",
        skill_stable,
        "Skill hashes match" if skill_stable else "Skill changed during run",
    )
    if isinstance(expectations.get("skill_should_be_loaded"), bool):
        should_load = expectations["skill_should_be_loaded"]
        add(
            "Zipline skill loaded" if should_load else "Zipline skill not loaded",
            loaded == should_load,
            "Observed skill file read" if loaded else "No skill file read observed",
        )
    if expectations.get("required_new_branch_before_file_change") is True:
        branch_passed, branch_detail = branch_before_first_file_change(
            trace, initial_branch, final_branch
        )
        add(
            "new branch checked out before first file change",
            branch_passed,
            branch_detail,
        )
    add(
        "file changes allowed",
        expectations["allow_file_changes"] or not files,
        "No files changed" if not files else f"{len(files)} file(s) changed",
    )
    file_list = ", ".join(file["path"] for file in files) or "No files changed"
    for glob in expectations.get("required_changed_globs", []):
        add(
            f"required changed file: {glob}",
            any(matches_glob(file["path"], glob) for file in files),
            file_list,
        )
    for glob in expectations.get("forbidden_changed_globs", []):
        add(
            f"forbidden changed file absent: {glob}",
            not any(matches_glob(file["path"], glob) for file in files),
            file_list,
        )

    command_text = "\n".join(str(command.get("command") or "") for command in trace["commands"])
    command_cursor = 0
    for pattern in expectations.get("required_command_patterns", []):
        match = re.search(pattern, command_text[command_cursor:], re.IGNORECASE)
        add(
            f"required command in order: {pattern}",
            match is not None,
            "Observed" if match else "Not observed",
        )
        if match:
            command_cursor += match.end()
    for pattern in expectations.get("forbidden_diff_patterns", []):
        add(
            f"forbidden diff pattern absent: {pattern}",
            re.search(pattern, diff, re.MULTILINE) is None,
            "Checked full patch",
        )
    for pattern in expectations.get("required_response_patterns", []):
        add(
            f"required response pattern: {pattern}",
            re.search(pattern, response, re.IGNORECASE | re.MULTILINE) is not None,
            "Checked final response",
        )
    return {"passed": all(check["passed"] for check in checks), "checks": checks}


def run_codex(
    *,
    codex_bin: str,
    model: str,
    reasoning_effort: str | None,
    approve_for_me: bool,
    sandbox: str,
    codex_args: list[str],
    worktree: Path,
    prompt: str,
    response_path: Path,
    timeout_seconds: int,
) -> RunResult:
    args = ["exec", "--ephemeral", "--json"]
    args += ["--approve-for-me"] if approve_for_me else ["--sandbox", sandbox]
    args += ["--model", model]
    if reasoning_effort:
        args += ["-c", f'model_reasoning_effort="{reasoning_effort}"']
    args += ["-C", str(worktree), "-o", str(response_path), *codex_args, "-"]
    return run(
        codex_bin,
        args,
        cwd=worktree,
        input_data=prompt,
        timeout_seconds=timeout_seconds,
    )


def create_fixture_baseline(repo_root: Path, worktree: Path, fixture_patch: str | None) -> str | None:
    if not fixture_patch:
        return None
    patch_path = (HARNESS_DIR / fixture_patch).resolve()
    if not patch_path.exists():
        raise FileNotFoundError(f"Fixture patch not found: {patch_path}")
    run_checked("git", ["apply", "--whitespace=nowarn", str(patch_path)], cwd=worktree)
    run_checked("git", ["add", "-A"], cwd=worktree)
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "Zipline Skill Eval",
            "GIT_AUTHOR_EMAIL": "skill-eval@localhost",
            "GIT_COMMITTER_NAME": "Zipline Skill Eval",
            "GIT_COMMITTER_EMAIL": "skill-eval@localhost",
        }
    )
    run_checked(
        "git",
        ["commit", "--no-gpg-sign", "-m", "Apply skill eval fixture"],
        cwd=worktree,
        env=env,
    )
    return run_checked("git", ["rev-parse", "HEAD"], cwd=worktree).stdout.decode().strip()


def prepare_checkout(
    repo_root: Path,
    checkout: Path,
    resolved_base: str,
    initial_branch: str | None,
) -> str:
    if initial_branch:
        run_checked(
            "git",
            [
                "clone",
                "--quiet",
                "--no-hardlinks",
                "--no-checkout",
                str(repo_root),
                str(checkout),
            ],
        )
        run_checked("git", ["switch", "-C", initial_branch, resolved_base], cwd=checkout)
        return "isolated-clone"
    run_checked("git", ["worktree", "add", "--detach", str(checkout), resolved_base], cwd=repo_root)
    return "worktree"


def execute_case(
    *,
    test_case: dict[str, Any],
    repetition: int,
    options: argparse.Namespace,
    repo_root: Path,
    suite_commit: str,
    tools: dict[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    run_id = f"{timestamp_id()}-{safe_id(test_case['id'])}-r{repetition}-{secrets.token_hex(3)}"
    run_dir = options.output_dir / run_id
    skill_dir = run_dir / "skill"
    skill_dir.mkdir(parents=True)

    skill_before = collect_skill_metadata(repo_root)
    if not skill_before["installed_matches_source"]:
        raise RuntimeError(
            f"Installed skill does not match {SKILL_SOURCE_RELATIVE}. "
            f"Source MD5={skill_before['md5']}; installed MD5={skill_before['installed_md5']}"
        )
    shutil.copyfile(skill_before["source_path"], skill_dir / "SKILL.md")
    (run_dir / "prompt.md").write_text(test_case["prompt"].strip() + "\n", encoding="utf-8")

    base_ref = options.base_ref or test_case.get("base_ref") or suite_commit
    resolved_base = run_checked(
        "git", ["rev-parse", f"{base_ref}^{{commit}}"], cwd=repo_root
    ).stdout.decode().strip()
    temp_root = Path(tempfile.mkdtemp(prefix="zipline-skill-eval-"))
    worktree = temp_root / "worktree"
    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "case_id": test_case["id"],
        "case_description": test_case["description"],
        "case_file": str(Path(test_case["case_file"]).relative_to(repo_root)),
        "status": "running",
        "input": {
            "model": options.model,
            "reasoning_effort": options.reasoning_effort or "inherited",
            "prompt_sha256": hashlib.sha256(test_case["prompt"].encode()).hexdigest(),
            "sandbox": options.sandbox,
            "approval_mode": "auto-review" if options.approve_for_me else "default",
            "codex_args": options.codex_args,
            "timeout_seconds": options.timeout_seconds,
            "repetition": repetition,
        },
        "skill": skill_before,
        "repository": {
            "root": str(repo_root),
            "requested_base_ref": base_ref,
            "base_commit": resolved_base,
            "fixture_commit": None,
        },
        "tools": tools,
        "codex_config_sha256": hash_optional_file(Path.home() / ".codex" / "config.toml"),
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "finished_at": None,
        "elapsed_ms": None,
        "worktree": str(worktree) if options.keep_worktrees else None,
    }
    write_json(run_dir / "manifest.json", manifest)

    codex_result = RunResult(1, None, False, b"", b"")
    cleanup_error = None
    checkout_mode = "worktree"
    try:
        checkout_mode = prepare_checkout(
            repo_root,
            worktree,
            resolved_base,
            test_case.get("initial_branch"),
        )
        manifest["repository"]["checkout_mode"] = checkout_mode
        manifest["repository"]["fixture_commit"] = create_fixture_baseline(
            repo_root, worktree, test_case.get("fixture_patch")
        )
        manifest["repository"]["baseline_commit"] = run_checked(
            "git", ["rev-parse", "HEAD"], cwd=worktree
        ).stdout.decode().strip()
        initial_branch = run_checked(
            "git", ["branch", "--show-current"], cwd=worktree
        ).stdout.decode().strip() or None
        manifest["repository"]["initial_branch"] = initial_branch

        response_path = run_dir / "response.md"
        codex_result = run_codex(
            codex_bin=options.codex_bin,
            model=options.model,
            reasoning_effort=options.reasoning_effort,
            approve_for_me=options.approve_for_me,
            sandbox=options.sandbox,
            codex_args=options.codex_args,
            worktree=worktree,
            prompt=test_case["prompt"],
            response_path=response_path,
            timeout_seconds=options.timeout_seconds,
        )
        (run_dir / "events.jsonl").write_bytes(codex_result.stdout)
        (run_dir / "stderr.log").write_bytes(codex_result.stderr)

        final_branch = run_checked(
            "git", ["branch", "--show-current"], cwd=worktree
        ).stdout.decode().strip() or None
        manifest["repository"]["final_branch"] = final_branch

        status = run_checked(
            "git", ["status", "--porcelain=v1", "-z", "--untracked-files=all"], cwd=worktree
        )
        files = parse_porcelain_v1_z(status.stdout)
        write_json(run_dir / "files.json", files)

        run_checked("git", ["add", "-N", "--", "."], cwd=worktree)
        diff_result = run_checked(
            "git", ["diff", "--binary", "--no-ext-diff", "HEAD"], cwd=worktree
        )
        diff = diff_result.stdout.decode("utf-8", errors="replace")
        (run_dir / "diff.patch").write_text(diff, encoding="utf-8")

        trace = parse_events(codex_result.stdout.decode("utf-8", errors="replace"))
        if not response_path.exists():
            response_path.write_text(trace["last_agent_message"], encoding="utf-8")
        response = response_path.read_text(encoding="utf-8")
        skill_after = collect_skill_metadata(repo_root)
        skill_stable = (
            skill_before["md5"] == skill_after["md5"]
            and skill_before["installed_md5"] == skill_after["installed_md5"]
            and skill_after["installed_matches_source"]
        )
        checks = evaluate_checks(
            test_case,
            files,
            diff,
            response,
            trace,
            codex_result.code,
            codex_result.timed_out,
            skill_stable,
            initial_branch,
            final_branch,
        )

        finished_at = datetime.now(timezone.utc)
        elapsed_ms = round((time.monotonic() - start_time) * 1000)
        result = {
            "schema_version": 1,
            "run_id": run_id,
            "case_id": test_case["id"],
            "exit_code": codex_result.code,
            "signal": codex_result.signal,
            "timed_out": codex_result.timed_out,
            "thread_id": trace["thread_id"],
            "tokens": trace["usage"],
            "elapsed_ms": elapsed_ms,
            "files_modified": files,
            "repository": {
                "checkout_mode": checkout_mode,
                "initial_branch": initial_branch,
                "final_branch": final_branch,
            },
            "skill_loaded": skill_loaded(trace),
            "skill_stable_during_run": skill_stable,
            "deterministic_evaluation": checks,
            "trace": {
                "event_count": trace["event_count"],
                "parse_errors": trace["parse_errors"],
                "commands": trace["commands"],
                "tool_calls": trace["tool_calls"],
                "actions": trace["actions"],
                "errors": trace["errors"],
            },
            "artifacts": {
                "manifest": "manifest.json",
                "prompt": "prompt.md",
                "skill_snapshot": "skill/SKILL.md",
                "events": "events.jsonl",
                "response": "response.md",
                "stderr": "stderr.log",
                "files": "files.json",
                "diff": "diff.patch",
                "judge_input": "judge-input.json",
            },
        }
        write_json(run_dir / "result.json", result)
        write_json(
            run_dir / "judge-input.json",
            {
                "schema_version": 1,
                "run_id": run_id,
                "case": {
                    "id": test_case["id"],
                    "description": test_case["description"],
                    "tags": test_case["tags"],
                    "prompt": test_case["prompt"],
                    "judge_criteria": test_case["expectations"]["judge_criteria"],
                },
                "environment": {
                    "model": options.model,
                    "reasoning_effort": options.reasoning_effort or "inherited",
                    "sandbox": options.sandbox,
                    "approval_mode": "auto-review" if options.approve_for_me else "default",
                    "repository_base_commit": resolved_base,
                    "repository_checkout_mode": checkout_mode,
                    "repository_initial_branch": initial_branch,
                    "repository_final_branch": final_branch,
                    "skill": {
                        key: skill_before[key]
                        for key in (
                            "md5",
                            "sha256",
                            "git_blob",
                            "last_commit",
                            "last_commit_date",
                            "source_dirty",
                        )
                    },
                    "tools": tools,
                },
                "outcome": {
                    "exit_code": codex_result.code,
                    "timed_out": codex_result.timed_out,
                    "elapsed_ms": elapsed_ms,
                    "tokens": trace["usage"],
                    "response": response,
                    "files_modified": files,
                    "skill_loaded": result["skill_loaded"],
                    "diff": diff,
                    "commands": trace["commands"],
                    "tool_calls": trace["tool_calls"],
                    "actions": trace["actions"],
                    "deterministic_evaluation": checks,
                },
            },
        )

        manifest["status"] = (
            "timed_out"
            if codex_result.timed_out
            else "completed"
            if codex_result.code == 0
            else "codex_failed"
        )
        manifest["finished_at"] = finished_at.isoformat().replace("+00:00", "Z")
        manifest["elapsed_ms"] = elapsed_ms
        manifest["skill_after"] = skill_after
        manifest["skill_stable_during_run"] = skill_stable
        write_json(run_dir / "manifest.json", manifest)
        return run_id, run_dir, result
    except Exception as error:
        manifest["status"] = "harness_failed"
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        manifest["elapsed_ms"] = round((time.monotonic() - start_time) * 1000)
        manifest["error"] = format_exception(error)
        write_json(run_dir / "manifest.json", manifest)
        if not (run_dir / "events.jsonl").exists():
            (run_dir / "events.jsonl").write_bytes(codex_result.stdout)
        if not (run_dir / "stderr.log").exists():
            (run_dir / "stderr.log").write_bytes(codex_result.stderr)
        raise
    finally:
        if not options.keep_worktrees and checkout_mode == "worktree" and worktree.exists():
            removal = run("git", ["worktree", "remove", "--force", str(worktree)], cwd=repo_root)
            if removal.code != 0:
                cleanup_error = removal.stderr.decode("utf-8", errors="replace").strip()
        if not options.keep_worktrees and temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)
        if cleanup_error:
            print(f"Warning: worktree cleanup failed: {cleanup_error}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    options = parse_args(argv if argv is not None else sys.argv[1:])
    repo_root = resolve_repo_root()
    suite_commit = run_checked("git", ["rev-parse", "HEAD"], cwd=repo_root).stdout.decode().strip()
    skill = collect_skill_metadata(repo_root)
    if not skill["installed_matches_source"]:
        raise RuntimeError(
            "Refusing to run: installed Zipline skill is stale. "
            f"Source MD5={skill['md5']}; installed MD5={skill['installed_md5']}"
        )

    tools = {
        "codex": get_version(options.codex_bin),
        "zipline": get_version("zipline"),
        "git": get_version("git"),
        "python": get_version(sys.executable, ["--version"]),
    }
    if not tools["codex"]["available"]:
        raise RuntimeError(f"Codex CLI is unavailable: {tools['codex']['output']}")

    cases = [test_case for test_case in load_cases() if test_case.get("enabled") is not False]
    if options.cases:
        selected = set(options.cases)
        cases = [test_case for test_case in cases if test_case["id"] in selected]
        found = {test_case["id"] for test_case in cases}
        missing = sorted(selected - found)
        if missing:
            raise ValueError(f"Unknown or disabled case(s): {', '.join(missing)}")
    if options.tag:
        cases = [test_case for test_case in cases if options.tag in test_case["tags"]]
    if not cases:
        raise ValueError("No enabled cases matched the requested filters")

    plan = {
        "model": options.model,
        "reasoning_effort": options.reasoning_effort or "inherited",
        "sandbox": options.sandbox,
        "approval_mode": "auto-review" if options.approve_for_me else "default",
        "repeat": options.repeat,
        "timeout_seconds": options.timeout_seconds,
        "repository": str(repo_root),
        "repository_commit": suite_commit,
        "skill": skill,
        "tools": tools,
        "output_dir": str(options.output_dir),
        "cases": [
            {
                "id": test_case["id"],
                "description": test_case["description"],
                "tags": test_case["tags"],
                "base_ref": options.base_ref or test_case.get("base_ref") or suite_commit,
                "initial_branch": test_case.get("initial_branch"),
            }
            for test_case in cases
        ],
    }
    if options.dry_run:
        print(json.dumps(plan, indent=2, ensure_ascii=False))
        return 0

    options.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(options.output_dir / f"suite-{timestamp_id()}.json", plan)
    failures: list[dict[str, Any]] = []
    for test_case in cases:
        for repetition in range(1, options.repeat + 1):
            print(f"Running {test_case['id']} ({repetition}/{options.repeat})...", flush=True)
            try:
                _, run_dir, result = execute_case(
                    test_case=test_case,
                    repetition=repetition,
                    options=options,
                    repo_root=repo_root,
                    suite_commit=suite_commit,
                    tools=tools,
                )
                status = "PASS" if result["deterministic_evaluation"]["passed"] else "CHECK"
                print(f"  {status} {run_dir}", flush=True)
            except Exception as error:
                failures.append(
                    {
                        "case_id": test_case["id"],
                        "repetition": repetition,
                        "error": format_exception(error),
                    }
                )
                print(f"  FAILED {test_case['id']}: {error}", file=sys.stderr, flush=True)
    if failures:
        write_json(options.output_dir / f"failures-{timestamp_id()}.json", failures)
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(format_exception(error), file=sys.stderr, end="")
        raise SystemExit(1)
