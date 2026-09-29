---
title: "Branching & Deployment Flow"
order: 4
---

# Branching & Deployment Flow

Zipline supports two broad models for deploying `GroupBy`s, `Join`s, and `StagingQuery`s: **orchestrate off `main`** and **orchestrate off a branch**. Both use the same `zipline hub schedule` / `zipline hub schedule-all` commands — the difference is *which git branch* those commands are invoked from, and *where in the lifecycle* that invocation happens.

This page covers when to reach for each model, what a developer's day-to-day flow looks like in each, and how descheduling works in both.

If you haven't already, read [Naming and Versioning](../authoring_features/NamingAndVersioning.md) and [Iterating](Iterate.md) first — this page assumes familiarity with version bumps and the compile/test/deploy loop.

## The two models at a glance

| | Orchestrate off `main` | Orchestrate off a branch |
|---|---|---|
| Who runs `schedule-all` | CI, on merge to `main` (see note below) | The developer, from their feature branch |
| What gets scheduled | Confs on `main` | Confs on the branch |
| Typical use case | Steady-state production features | A/B test branches, experiments, shadow deployments |
| Reviewability | PR review + git blame + CI logs make it auditable which configs are live in prod | The branch state is the source of truth — no single place to audit "what's live" |
| Promotion to prod | No action needed if the experiment conf was already merged to `main` | Merge the branch to `main` |

Most teams should default to orchestrating off `main` and only reach for the branch flow for experiments they don't want to merge yet.

## Orchestrate off `main`

Production jobs are driven by `main`. The recommended setup is for teams to wire a CI step that runs `zipline hub schedule-all` on every merge to `main` — Zipline itself doesn't watch your repo, so this hookup has to be configured by you. Once it's in place, `schedule-all` is authoritative for `main`'s schedules on the hub: any conf in `compiled/` gets its schedule (re)deployed, and any conf that disappeared from `compiled/` has its hub schedule pruned.

### Typical developer flow

1. Cut a PR branch off `main`.
2. Fork the entity you want to change. If your production `Join` is `joins/recs/user_features.py` with `v1 = Join(...)`, add a `v2 = Join(...)` next to it (or in a new file) rather than editing `v1` in place. Similarly, fork any `GroupBy`s you're changing so they can coexist with the old versions.
3. Iterate against the branch using the standard `zipline compile` / `zipline hub eval` / `zipline hub backfill` loop — see [Testing](Test.md) for details. This does not touch production schedules.
4. Open a PR, get it reviewed, merge.
5. Your post-merge CI job runs `zipline hub schedule-all` on `main`. The new `v2` gets scheduled alongside the existing `v1`.

Because the old and new versions coexist in `compiled/` on `main`, both stay scheduled and both are fetchable — this is what lets you run an A/B test off `main`.

### Descheduling in this model

When the experiment is done and you're ready to turn off the old version, set `online=False` (or `online_schedule="@never"` and `offline_schedule="@never"`) on the superseded conf, PR, merge, and let the next post-merge `schedule-all` run retire the schedule. You can delete the config file entirely in a follow-up PR (or leave it — `schedule-all` prunes any hub row whose conf is no longer in the compile output).

If the experiment *is* promoted to production, no additional action is needed: the winning version is already on `main` and already scheduled. There's nothing to "promote."

## Orchestrate off a branch

Sometimes you want an experiment's jobs running — daily backfills, batch uploads, streaming updates — *without* merging to `main` yet.

Running `zipline hub schedule-all` from a branch registers schedules against *that branch* on the hub. The schedules coexist with `main`'s schedules; fetches for confs on the branch route to the branch's jobs.

### Typical developer flow

1. Cut a branch off `main` for the experiment.
2. Change the relevant entities. You have two options:
   - **Bump the version in place** (change `v1 = Join(..., version=1)` to `version=2`). Preferred for branch experiments — the diff is small, downstream references stay pointed at the same variable, and merging back to `main` cleanly supersedes the old version.
   - **Fork the entity** (add `v2 = Join(...)` next to the existing `v1`). Use this when you specifically want the two versions to coexist after merge (e.g. long-running A/B where both variants remain production).
3. `zipline compile` and iterate.
4. Run `zipline hub schedule-all` from the branch yourself. This deploys schedules under the branch name on the hub, keeping daily jobs running and streaming jobs alive. Re-run it whenever you make further changes on the branch that you want reflected in the running schedules.

### Promoting a branch experiment to production

Once the experiment is winning and you want to make it the mainstream version:

1. While the branch is still deployed, shift downstream traffic (fetch calls, downstream `Join`s) over to the branch's version so the cutover happens under live serving.
2. Merge the branch to `main`.
3. CI's post-merge `zipline hub schedule-all` on `main` picks up the new version. Because `schedule-all` is authoritative per branch, the old `main` version's schedule rows are retired automatically — deploying the new version supersedes prior versions of the same base conf, and any conf that dropped out of `compiled/` is pruned.

No manual descheduling is needed for the superseded version. (See the streaming caveat below — the underlying streaming job for the old version still needs to be stopped by hand today.)

### Descheduling in this model

Switch to the experiment branch, set `online=False` (or the `"@never"` schedules) on the confs you want to retire, and re-run `zipline hub schedule-all`. The hub retires the schedule rows for that branch.

### Caveat: version bumps and orphaned streaming jobs

`schedule-all` is authoritative per branch: it prunes any hub schedule row whose conf name is no longer in the local compile output. Because a version bump changes the conf name (`user_features.v1__1` → `user_features.v1__2`), the old row *is* pruned automatically.

**However, pruning the schedule row does not currently tear down the underlying streaming job.** If the old version had a live streaming job, stop it manually (via the Zipline Hub UI or your cluster's job manager) after the new version is scheduled.

## Multi-environment (`prod` / `canary`)

Both models compose with the `--env` flag. `zipline hub schedule-all --env canary` reads `compiled_canary/` and deploys only the confs whose `environments=` list contains `canary`. Everything above applies: `--env prod` runs from CI on `main` for the steady-state case, and developers can run `--env canary` from a branch for canary experiments.

One operational note: whichever machine runs `schedule-all` needs access to reach the target env's hub — CI needs prod access to deploy prod schedules, and whoever runs `--env canary` needs canary access.

See [Multi-Environment Compile & Deploy](MultiEnvironment.md) for the full guide on `teams.canary.py`, per-entity `environments=[...]` opt-in, and the `--env` behavior matrix.
