#!/usr/bin/python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


HARNESS_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a JSONL batch of Zipline skill judge inputs.")
    parser.add_argument("--runs-dir", type=Path, default=HARNESS_DIR / "runs")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-failed", action="store_true")
    return parser.parse_args()


def skill_was_loaded(payload: dict[str, Any]) -> bool:
    commands = payload.get("outcome", {}).get("commands", [])
    return any(
        "/skills/zipline/SKILL.md" in str(command.get("command") or "")
        for command in commands
    )


def main() -> int:
    options = parse_args()
    runs_dir = options.runs_dir.expanduser().resolve()
    output_path = (
        options.output.expanduser().resolve()
        if options.output
        else runs_dir / "judge-batch.jsonl"
    )
    if not runs_dir.exists():
        raise SystemExit(f"Runs directory does not exist: {runs_dir}")

    payloads: list[str] = []
    for run_dir in sorted(path for path in runs_dir.iterdir() if path.is_dir()):
        judge_path = run_dir / "judge-input.json"
        manifest_path = run_dir / "manifest.json"
        if not options.include_failed and manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("status") != "completed":
                continue
        if not judge_path.exists():
            continue
        payload = json.loads(judge_path.read_text(encoding="utf-8"))
        outcome = payload.get("outcome")
        if isinstance(outcome, dict) and not isinstance(outcome.get("skill_loaded"), bool):
            outcome["skill_loaded"] = skill_was_loaded(payload)
        payloads.append(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(payloads) + ("\n" if payloads else ""), encoding="utf-8")
    print(f"Wrote {len(payloads)} judge payload(s) to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
