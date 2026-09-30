# Zipline skill evaluations

This harness evaluates the Zipline skill in isolated Git worktrees and captures
enough information to compare skill revisions or send runs to an LLM-as-judge.
Case prompts and criteria describe agent behavior without assuming a particular
coding agent. The current runner uses the Codex CLI and its event format.

The harness supports Python 3.9+ and uses only the standard library. The
examples use `/usr/bin/python3` to avoid project-local version-manager shims;
another Python 3.9+ interpreter works as well.

The canonical skill source is:

```text
python/src/ai/chronon/resources/agent_context.md
```

The harness verifies that it is byte-for-byte identical to the installed Codex
skill at `~/.codex/skills/zipline/SKILL.md` before starting a run. This avoids
silently evaluating a stale installed copy.

Runs use Codex automatic approval review inside the `workspace-write` sandbox.
Pass `--no-approve-for-me` to test the CLI's default approval behavior instead.

## Validate without running Codex

```bash
/usr/bin/python3 skill-evals/zipline/run_evals.py \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --dry-run
```

## Run one case

```bash
/usr/bin/python3 skill-evals/zipline/run_evals.py \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --case diagnose-null-join
```

## Run a tagged suite three times

```bash
/usr/bin/python3 skill-evals/zipline/run_evals.py \
  --model gpt-5.6-sol \
  --reasoning-effort high \
  --tag offline \
  --repeat 3
```

Runs are written to `skill-evals/zipline/runs/` by default. Each run contains:

- `manifest.json`: model, repository, skill, tool, and timing metadata.
- `prompt.md`: the exact prompt passed to Codex.
- `skill/SKILL.md`: the exact skill snapshot used for the run.
- `events.jsonl`: the raw `codex exec --json` event stream.
- `response.md`: the final Codex response.
- `stderr.log`: Codex progress and diagnostics.
- `files.json`: files created, modified, deleted, renamed, or copied.
- `diff.patch`: the complete binary-safe Git diff from the clean baseline.
- `result.json`: normalized tokens, timing, checks, and artifact paths.
- `judge-input.json`: the prompt, response, complete diff, trace summary, and
  evaluation criteria in one LLM-as-judge payload.

Use `--output-dir <path>` to place runs elsewhere and `--keep-worktrees` to
retain evaluated worktrees for debugging. Run `--help` for all options.
The default per-run timeout is 900 seconds and can be changed with
`--timeout-seconds`.

Cases are JSON files in `cases/` and are validated by the runner. The schema in
`schemas/case.schema.json` documents their shape. Integration cases can invoke
the Zipline eval service or warehouse; offline cases should be usable without
those dependencies.

## Behavior coverage

The suite has 30 cases. The 14 cases tagged `original-context` cover behaviors
whose examples or workflows were shortened when `agent_context.md` was condensed.
Prompts supply user intent and necessary facts; judge criteria describe the
expected behavior separately so the prompt does not give away the solution.

| Original context topic | Cases | What the judge checks |
|---|---|---|
| Reuse and version GroupBys | `add-groupby-feature`, `reuse-warehouse-export` | Reuse source/key pairs and existing exports; preserve published configs. |
| Branch before edits | `branch-before-config-change` | Leave `main` before changing files; no agent-specific branch prefix is required. |
| Table discovery | `ambiguous-table-name` | Resolve ambiguous names before editing. |
| Time column handling | `time-column-formats`, `diagnose-null-join` | Convert seconds/strings, preserve milliseconds, and verify rather than assume units. |
| Aggregations and filtering | `aggregation-input-expression`, `feature-specific-filtering` | Select expressions before aggregation; preserve other feature populations and average denominators. |
| Entity snapshots and mutations | `entity-mutations` | Choose the source mode and validate mutation time and before-image semantics. |
| Join keys and derivations | `join-key-mapping-derivations` | Map keys in the correct direction, expose row IDs, and preserve requested columns. |
| Staging queries and external warehouses | `staging-query-dependencies`, `reuse-warehouse-export` | Correct engine, dependency declarations, partition bounds, and exported table references. |
| Chained GroupBys | `chained-groupby`, `chained-groupby-unbounded` | Parent output columns, timestamp, streaming windows, serving dependencies, and validation order. |
| CLI operations and historical output | `groupby-backfill`, `groupby-backfill-invalid-range` | Correct backfill command, dates, source history, and submission/completion distinction. |
| Model inference | `model-transform-contract` | JoinSource input, raw inference instance, response schema, and output field path. |
| Incorrect values | `diagnose-incorrect-value` | Recompute a specific key/window and investigate duplicates without guessing a fix. |
| Tecton migration | `tecton-migration` | Source mapping, windows, approximate distinct counts, buckets, SQL derivations, and bottom-up validation. |
| Offline-only authoring | `non-production-groupby-online`, `non-production-join-online` | Keep development experiments offline. |
| Skill scope | `unrelated-python-change` | Avoid triggering feature-authoring behavior for ordinary Python review. |
| Implicit model comparisons | `trial-comparison-plan`, `trial-not-needed` | Open a trial for comparisons, search prior work, bind a real model, record before training, and avoid trials for smoke runs/backfills. |
| Measured results | `trial-metrics-provenance`, `trial-incomplete-comparison` | Read trainer reports through Hub; do not submit metrics, trust estimates, or claim unfinished comparisons succeeded. |
| Variant isolation and artifacts | `trial-variant-isolation`, `trial-archive-publication` | Distinct model/report locations, name-scoped archives, content versions, and recovery from shared reports or missing archives. |
| Trial continuity and updates | `trial-resume`, `trial-partial-update` | Resume by ID, propagate attribution across shells, parse JSON, preserve omitted fields, and use narrow setters. |
| Comparison handoff | `trial-summary-journal` | Ground conclusions in retrieved measurements and preserve the outcome in a summary, status, and journal entry. |

Read-only cases assess the agent's proposed corrections and operational plans;
authoring cases assess actual edits plus compile/eval attempts. Deterministic
checks enforce file/command/response constraints, while the judge evaluates
semantics and user experience. Passing deterministic checks alone does not prove
behavioral equivalence. Custom model training/deployment and a full multi-hop
pipeline rollout are not yet covered end to end.

The nine cases tagged `trials` are offline plans/reviews of the workflow added on
`main`. They assess proposed behavior and supplied report evidence without
creating Hub records, publishing archives, or running cloud training. They do not
establish that the live training-to-report integration works. Run just this group
with `--tag trials`; add `--dry-run` to validate case selection without invoking
an agent. Prompts live separately from judge criteria; when running behavioral
evals, keep the case/criteria files out of the agent's fixture so repository
searches cannot reveal the rubric.

To compare the original and shortened skill, keep the case suite, repository
`--base-ref`, model, reasoning effort, and service availability fixed. Install
each candidate skill so it matches the canonical source for that run, and use
separate output directories. The runner snapshots and hashes the skill; changing
`--base-ref` alone changes the repository fixture, not the installed skill. Compare
the same case IDs and repetitions with the judge, including failures and
unnecessary clarification or tool calls, before claiming equivalent behavior.

For the added coverage, use `--tag original-context`; for checks that need no live
services, use `--tag offline`. No live before/after results are implied by adding
a case to this table.

## Branch-order checks

A case with `initial_branch` runs in a temporary local clone so it can safely
start on a named branch such as `main` without touching the real repository's
refs. Set `expectations.required_new_branch_before_file_change` to require a
successful `git switch -c`/`git checkout -b`, a different final branch, and an
ordered trace showing that branch creation completed before the first
`file_change` event.

## Judge inputs and summaries

Build a JSONL file for the judge with:

```bash
/usr/bin/python3 skill-evals/zipline/build_judge_batch.py
```

Failed or incomplete Codex runs are excluded by default. Pass `--include-failed`
when failure analysis is itself part of the judging task.

Summarize all recorded runs with:

```bash
/usr/bin/python3 skill-evals/zipline/summarize.py
```
