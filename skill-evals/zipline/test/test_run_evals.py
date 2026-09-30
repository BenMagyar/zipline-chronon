from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


RUNNER_PATH = Path(__file__).resolve().parents[1] / "run_evals.py"
SPEC = importlib.util.spec_from_file_location("zipline_run_evals", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
RUNNER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RUNNER
SPEC.loader.exec_module(RUNNER)


def event_stream(branch_before_change: bool) -> str:
    branch_event = {
        "type": "item.completed",
        "item": {
            "id": "branch",
            "type": "command_execution",
            "command": "git switch -c feature/add-feature",
            "status": "completed",
            "exit_code": 0,
        },
    }
    file_event = {
        "type": "item.started",
        "item": {
            "id": "edit",
            "type": "file_change",
            "status": "in_progress",
        },
    }
    ordered = [branch_event, file_event] if branch_before_change else [file_event, branch_event]
    return "\n".join(json.dumps(event) for event in ordered)


class BranchOrderingTest(unittest.TestCase):
    def test_branch_creation_before_file_change_passes(self) -> None:
        trace = RUNNER.parse_events(event_stream(branch_before_change=True))

        for initial_branch in ("main", "develop", "feature/existing"):
            with self.subTest(initial_branch=initial_branch):
                passed, detail = RUNNER.branch_before_first_file_change(
                    trace, initial_branch, "feature/add-feature"
                )

                self.assertTrue(passed, detail)

    def test_missing_initial_branch_fails(self) -> None:
        trace = RUNNER.parse_events(event_stream(branch_before_change=True))

        for initial_branch in (None, ""):
            with self.subTest(initial_branch=initial_branch):
                passed, detail = RUNNER.branch_before_first_file_change(
                    trace, initial_branch, "feature/add-feature"
                )

                self.assertFalse(passed, detail)

    def test_branch_creation_after_file_change_fails(self) -> None:
        trace = RUNNER.parse_events(event_stream(branch_before_change=False))

        passed, _ = RUNNER.branch_before_first_file_change(
            trace, "main", "feature/add-feature"
        )

        self.assertFalse(passed)

    def test_remaining_on_main_fails(self) -> None:
        trace = RUNNER.parse_events(event_stream(branch_before_change=True))

        passed, _ = RUNNER.branch_before_first_file_change(trace, "main", "main")

        self.assertFalse(passed)


if __name__ == "__main__":
    unittest.main()
