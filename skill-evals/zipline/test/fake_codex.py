#!/usr/bin/python3

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def value_after(args: list[str], flag: str) -> str | None:
    try:
        return args[args.index(flag) + 1]
    except (ValueError, IndexError):
        return None


def main(args: list[str]) -> int:
    if "--version" in args:
        print("fake-codex 1.0.0")
        return 0
    if not args or args[0] != "exec":
        print(f"Unexpected fake Codex arguments: {' '.join(args)}", file=sys.stderr)
        return 2

    worktree_value = value_after(args, "-C")
    response_value = value_after(args, "-o")
    if not worktree_value or not response_value:
        print("Fake Codex requires -C and -o", file=sys.stderr)
        return 2

    prompt = sys.stdin.read()
    worktree = Path(worktree_value)
    response_path = Path(response_value)
    initial_branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    setup_events = [
        {
            "type": "item.completed",
            "item": {
                "id": "command-branch-check",
                "type": "command_execution",
                "command": "git branch --show-current",
                "status": "completed",
                "exit_code": 0,
            },
        }
    ]
    if initial_branch == "main":
        subprocess.run(
            ["git", "switch", "-c", "feature/fake-eval-branch"],
            cwd=worktree,
            check=True,
            capture_output=True,
            text=True,
        )
        setup_events.append(
            {
                "type": "item.completed",
                "item": {
                    "id": "command-branch-create",
                    "type": "command_execution",
                    "command": "git switch -c feature/fake-eval-branch",
                    "status": "completed",
                    "exit_code": 0,
                },
            }
        )
    changed_file = (
        worktree
        / "python"
        / "test"
        / "canary"
        / "group_bys"
        / "gcp"
        / "user_activities.py"
    )
    with changed_file.open("a", encoding="utf-8") as target:
        target.write("\n# fake-codex plumbing test change\n")

    response = f"Fake Codex completed the plumbing test for a {len(prompt)}-character prompt.\n"
    response_path.write_text(response, encoding="utf-8")
    events = [
        {"type": "thread.started", "thread_id": "fake-thread"},
        {"type": "turn.started"},
        *setup_events,
        {
            "type": "item.completed",
            "item": {
                "id": "command-0",
                "type": "command_execution",
                "command": "sed -n '1,3200p' /tmp/.codex/skills/zipline/SKILL.md",
                "status": "completed",
                "exit_code": 0,
            },
        },
        {
            "type": "item.started",
            "item": {
                "id": "file-change-0",
                "type": "file_change",
                "status": "in_progress",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "file-change-0",
                "type": "file_change",
                "status": "completed",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "command-1",
                "type": "command_execution",
                "command": "zipline compile --force",
                "status": "completed",
                "exit_code": 0,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "command-2",
                "type": "command_execution",
                "command": "zipline hub eval --skip-compile compiled/group_bys/gcp/user_activities.v2__1",
                "status": "completed",
                "exit_code": 0,
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "message-1",
                "type": "agent_message",
                "text": response.strip(),
            },
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": 20,
                "output_tokens": 30,
                "reasoning_output_tokens": 10,
            },
        },
    ]
    for event in events:
        print(json.dumps(event, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
