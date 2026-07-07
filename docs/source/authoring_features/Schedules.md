---
title: "Schedules"
order: 5.5
---

# Schedules

Chronon schedules define when a `StagingQuery`, `GroupBy`, or `Join` should
materialize data. The schedule is separate from the partition grid: the schedule
decides when a job fires, while the grid decides which output interval that job
writes.

Daily is the default. Existing daily configs do not need to declare any extra
partition settings.

## Schedule fields

Chronon configs use two schedule fields:

| Field | Meaning |
|---|---|
| `offline_schedule` | Runs offline compute, such as staging query output, groupBy snapshots, and join backfills. |
| `online_schedule` | Runs online deploy/upload work when `online=True`. |

Use `@never` to disable a schedule explicitly.

For sub-daily online configs, leave `online_schedule` unset unless you need to
disable it with `@never`. When `online=True`, Chronon inherits the sub-daily
`offline_schedule`; if both are set, they must match.

## Schedule lifecycle

Your config is the source of truth for which schedules exist. Every
`zipline hub schedule-all` run (typically CI, post-merge) brings the hub in
line with the repo for the branch it runs on:

- A conf with a schedule gets one, or keeps the one it has.
- A conf whose schedules are `@never` or unset has its schedules removed.
- A conf that no longer exists in the repo — deleted, or superseded by a
  version bump — has its schedules removed. Bumping `version=3` to
  `version=4` retires the `__3` schedules automatically; there is nothing to
  clean up by hand.

Runs from one branch never touch schedules that another branch deployed.
Scheduling an experimental version from a feature branch alongside the
production version is safe; when the branch merges, the production
`schedule-all` takes over and retires the superseded version.

There is deliberately no out-of-band pause: stopping a schedule means changing
the config (`@never` or remove it) and merging, so every schedule change goes
through review and nothing stays silently held.

## Partition grids

Every output table is written on a partition grid.

| Concept | Meaning |
|---|---|
| Partition `ds` | The formatted UTC instant of the interval start, such as `2026-06-03-04-00`. |
| Grid | `partition_interval` plus `partition_offset`. A `3h` interval with `1h` offset has boundaries at 01:00, 04:00, 07:00, and so on. |
| Data held | Each partition holds `[start, start + interval)`. |
| Schedule | A cron expression that says when to fire. It does not define the grid. |
| Delay | The cron fire phase relative to the grid. A 04:15 fire over a `3h` grid at `1h` offset processes the partition that closed at 04:00. |

The grid is part of the data contract. The schedule is part of execution. This
separation lets a job wait for late-arriving data without changing which
partition it produces.

Two rules govern grids:

- `partition_interval` must be exactly `1d`, or a supported sub-daily interval
  that divides a UTC day evenly and can be represented by a simple repeating
  cron field. Common examples are `15m`, `30m`, `1h`, `2h`, `3h`, `4h`, `6h`,
  `8h`, and `12h`. Intervals such as `45m` and `90m` are not supported.
- `partition_offset` must satisfy `0 <= offset < interval`. A nonzero offset,
  including on a `1d` interval, uses timestamp-shaped partition labels such as
  `yyyy-MM-dd-HH-mm` and follows the grid-aware path.

Week- or month-sized partitions are not supported. Weekly and monthly jobs are
schedules over daily partitions.

## Valid schedules

Daily-or-coarser schedules run over daily partitions:

```python
offline_schedule="@daily"
offline_schedule="0 2 * * *"      # daily at 02:00 UTC
offline_schedule="0 6 * * MON"    # weekly, still over daily partitions
offline_schedule="0 6 1 * *"      # monthly, still over daily partitions
```

Sub-daily schedules must fire regularly across the whole UTC day, with `*` in
the day-of-month, month, and weekday fields:

```python
offline_schedule="0 */3 * * *"    # every 3 hours
offline_schedule="*/15 * * * *"   # every 15 minutes
offline_schedule="5/15 * * * *"   # every 15 minutes, 5 minute processing delay
```

These are rejected:

```python
offline_schedule="0 */5 * * *"    # 5h does not tile a UTC day evenly
offline_schedule="0 */2 * * MON"  # sub-daily but only on Mondays
offline_schedule="*/45 * * * *"   # irregular simple-cron interval
```

## Declaring output cadence

For a regular sub-daily schedule, Chronon infers the output interval from the
schedule:

```python
hourly = GroupBy(
    sources=[...],
    keys=["user_id"],
    aggregations=[...],
    online=True,
    offline_schedule="0 * * * *",
)
```

The hourly schedule above writes partitions such as `2026-06-03-04-00`, each
holding one hour of data.

Declare `partition_interval` when there is no enabled schedule, or when you want
the code to state the grid explicitly:

```python
three_hour = GroupBy(
    sources=[
        EventSource(
            table="data.checkouts",
            query=Query(
                time_column="ts",
                partition_interval="3h",
                partition_offset="1h",
            ),
        )
    ],
    keys=["user_id"],
    aggregations=[...],
    online=True,
    partition_interval="3h",
    partition_offset="1h",
    offline_schedule="15 1-22/3 * * *",
)
```

This grid has boundaries at 01:00, 04:00, 07:00, and so on. The 04:15 fire
writes the `...-01-00` partition, which holds `[01:00, 04:00)`. The `15` minute
cron phase is processing delay; it does not move the grid. If your data
boundaries are offset from midnight, declare that with `partition_offset`.
When `partition_offset` is provided without `partition_interval`, Chronon assumes
a `1d` interval on that offset grid.

When both `partition_interval` and an enabled sub-daily schedule are present,
the interval must match the cron data interval. A daily-or-coarser schedule over
a sub-daily partition interval is rejected because it would be ambiguous whether
one fire should write one partition, all elapsed partitions, or only the latest
closed partition.

## Dependencies

Chronon carries output and dependency partition specs separately because one
node's output table and its input tables can use different partition labels,
formats, intervals, or offsets.

Users normally only need to set dependency partition specs for external tables.
Chronon-produced tables carry their grid metadata when referenced through the
Chronon table reference, such as `producer.table` or `join.derived_table`. Avoid
string-building Chronon table names; a string like `f"{join.table}__derived"`
drops the metadata and may require you to declare the grid manually.

For external tables:

- Physical Hive-style tables feeding a sub-daily output should declare
  `partition_interval` and, if needed, `partition_offset` on the source
  `Query` or `TableDependency`.
- `time_partitioned=True` tables use a timestamp or date column for readiness
  and scans. They can omit `partition_interval` and `partition_offset`; Chronon
  slices them on the consumer's grid.
- Custom `triggerExpr` remains the escape hatch for custom readiness checks.

Example external source:

```python
source = EventSource(
    table="vendor.events",
    query=Query(
        time_column="event_ts",
        partition_column="ds",
        partition_interval="1h",
    ),
)
```

Example continuously landed timestamp table:

```python
source = EventSource(
    table="vendor.events",
    query=Query(
        time_column="event_ts",
        partition_column="event_ts",
        time_partitioned=True,
    ),
)
```

## Backfill correctness

For every run, Chronon internally converts the requested range into
`startMillis` and `endMillis`, then resolves that interval into the partition
labels for each table involved. This is why a 3-hour output can read hourly
source partitions without assuming the source uses the same `ds` labels as the
output.

In the examples below, **downstream** means the config whose output partition is
being materialized. For a `GroupBy`, the downstream grid is the `GroupBy` output
grid and the upstream grids are its source tables. For a `StagingQuery`, the
downstream grid is the staging query output grid and the upstream grids are its
table dependencies. For a `Join`, this whole-partition alignment rule applies
to the join output and its left-side dependencies; join right parts follow the
separate row-time binding rule below.

For sources that Chronon reads as whole partitions, downstream boundaries must
line up with upstream boundaries. Otherwise the downstream partition would cut
through an upstream partition and Chronon could not read exactly the needed
input range by partition label alone.

Valid example:

```text
downstream: 6h interval, 4h offset -> boundaries 04:00, 10:00, 16:00, 22:00
upstream:   3h interval, 1h offset -> boundaries 01:00, 04:00, 07:00, 10:00, ...

The downstream [04:00, 10:00) output reads whole upstream partitions
[04:00, 07:00) and [07:00, 10:00).
```

Invalid example:

```text
downstream: 6h interval, 2h offset -> boundaries 02:00, 08:00, 14:00, 20:00
upstream:   3h interval, 1h offset -> boundaries 01:00, 04:00, 07:00, 10:00, ...

The downstream [02:00, 08:00) output cuts through upstream partitions
[01:00, 04:00) and [07:00, 10:00), so a whole-partition read would over-read.
```

Join right parts are different. A join's output grid and left source grid can
be independent of the grids used by the `GroupBy`s on the right. Snapshot right
parts are bound as-of each row's timestamp on the right part's own grid.
Temporal right parts keep temporal point-in-time behavior. This lets one join
combine realtime, sub-daily, and daily features without requiring the join grid
to line up with every right part grid.

```python
training_set = Join(
    left=EventSource(
        table="data.checkouts",
        query=Query(time_column="ts", partition_interval="3h"),
    ),
    right_parts=[
        JoinPart(group_by=realtime_gb),      # temporal/as-of per event
        JoinPart(group_by=three_hour_gb),    # snapshot on a 3h grid
        JoinPart(group_by=daily_dims_gb),    # snapshot refreshed daily
    ],
    partition_interval="3h",
)
```

A daily snapshot part under a 3-hour join means bounded staleness: rows use the
latest daily snapshot at or before their timestamp. It is not treated as missing
data.

The same is true for unaligned sub-daily right parts. For example, an hourly
join over a left table partitioned at `00:00, 01:00, 02:00, ...` can include a
snapshot `GroupBy` on a `3h` grid offset by `1h` (`01:00, 04:00, 07:00, ...`).
A row at `12:07` binds to the latest right-part snapshot at or before that row
time, even though the join's hourly output boundary does not line up with the
right part's 3-hour grid.

## Partition formats

Output `ds` values default to:

| Grid | Format |
|---|---|
| Daily | `yyyy-MM-dd` |
| Sub-daily | `yyyy-MM-dd-HH-mm` |

The sub-daily default is dash-separated because partition values become object
store directory names. Spaces and colons tend to URL-escape in those paths.
External input tables can still declare their actual partition format.

Changing `partition_interval` or `partition_offset` changes the output grid and
therefore the output table shape. Treat that as a version bump. Schedule changes
only change when the job runs; they do not change the output grid unless the
grid was being inferred from the schedule.

## Manual runs

Hub CLI partition inputs accept daily and sub-daily values. Daily inputs are
sent as `YYYY-MM-DD`; sub-daily inputs are normalized to `YYYY-MM-DD-HH-mm`.

```sh
--start-ds 2024-01-15-03 --end-ds 2024-01-15-06-30
--start-ds "2024-01-15 03:00" --end-ds "2024-01-15T06:30"
--start-ds "2024/01/15 03:00:00" --end-ds "2024-01-15-06:30"
```

Second precision is only accepted when seconds are `00`, because Chronon
partitions are minute-aligned.

## Rules at a glance

1. Daily is the default. Sub-daily grids use `yyyy-MM-dd-HH-mm` by default.
2. A schedule tells Chronon when to run; `partition_interval` and
   `partition_offset` define the data grid.
3. Sub-daily schedules must be regular across the whole UTC day and cannot
   restrict day-of-month, month, or weekday.
4. Users set dependency partition specs for external tables. Chronon-produced
   table references carry their own grid metadata.
5. External `time_partitioned=True` inputs can omit a grid; Chronon slices them
   on the consumer grid.
6. Full-partition source reads require the materialized node's output
   boundaries to line up with its source or dependency boundaries.
7. Join right parts bind independently by row timestamp. The join grid and left
   grid do not need to line up with right `GroupBy` output or source grids.
8. Changing `partition_interval` or `partition_offset` changes the output shape
   and should be versioned.
9. Config declares which schedules exist: `schedule-all` removes schedules for
   confs that were unscheduled, deleted, or version-bumped. There is no
   out-of-band pause — schedule changes go through config review.
