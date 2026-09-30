#!/usr/bin/python3

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


HARNESS_DIR = Path(__file__).resolve().parent


def format_table(rows: list[dict[str, Any]]) -> str:
    headers = ["run", "case", "exit", "checks", "files", "input_tokens", "output_tokens", "seconds"]
    values = [[str(row[header]) for header in headers] for row in rows]
    widths = [
        max(len(header), *(len(row[index]) for row in values))
        for index, header in enumerate(headers)
    ]

    def render(row: list[str]) -> str:
        return " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    separator = "-+-".join("-" * width for width in widths)
    return "\n".join([render(headers), separator, *(render(row) for row in values)])


def main(argv: list[str]) -> int:
    runs_dir = Path(argv[0]).expanduser().resolve() if argv else (HARNESS_DIR / "runs").resolve()
    if not runs_dir.exists():
        raise SystemExit(f"Runs directory does not exist: {runs_dir}")

    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in runs_dir.iterdir() if path.is_dir()):
        result_path = run_dir / "result.json"
        if not result_path.exists():
            continue
        result = json.loads(result_path.read_text(encoding="utf-8"))
        evaluation = result.get("deterministic_evaluation") or {}
        tokens = result.get("tokens") or {}
        rows.append(
            {
                "run": result.get("run_id", ""),
                "case": result.get("case_id", ""),
                "exit": result.get("exit_code", ""),
                "checks": "pass"
                if result.get("exit_code") == 0 and evaluation.get("passed")
                else "fail",
                "files": len(result.get("files_modified") or []),
                "input_tokens": tokens.get("input_tokens", ""),
                "output_tokens": tokens.get("output_tokens", ""),
                "seconds": f"{(result.get('elapsed_ms') or 0) / 1000:.1f}",
            }
        )

    if rows:
        print(format_table(rows))
    else:
        print(f"No completed runs found in {runs_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
