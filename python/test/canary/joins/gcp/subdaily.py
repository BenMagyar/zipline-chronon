from group_bys.gcp import dim_listings, dim_merchants
from staging_queries.gcp.exports import user_activities as exports_user_activities

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

"""
Join with a sub-daily (hourly) output partition grid.

The output lands on an hourly grid, so its partition values use the "yyyy-MM-dd-HH-mm"
format. Three things are required for a sub-daily output:
- partition_interval="1h" sets the output grid cadence.
- offline_schedule must be a regular cron at that cadence ("0 * * * *"); the default
  "@daily" is coarser than the grid and is rejected.
- the left source must declare its own grid. The BigQuery export is timestamp-filtered
  rather than Hive ds-partitioned, so time_partitioned=True lets the join read it without
  a declared sub-daily ds interval.

Right parts are SNAPSHOT dimension lookups: each picks the latest daily snapshot at or
before a left row's timestamp, so a daily part under an hourly join is fine (mixed cadence
is expected). A TEMPORAL event-source part would instead need its own source grid declared
(partition_interval or time_partitioned) so the planner can sense readiness on the grid.
"""

source = EventSource(
    table=exports_user_activities.table,
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            row_id="event_id",
        ),
        time_column="event_time_ms",
        time_partitioned=True,
    ),
)

v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=dim_listings.v1),
        JoinPart(group_by=dim_merchants.v1, prefix="merchant_"),
    ],
    version=1,
    online=True,
    output_namespace="data",
    step_days=30,
    partition_interval="1h",
    offline_schedule="0 * * * *",
)
