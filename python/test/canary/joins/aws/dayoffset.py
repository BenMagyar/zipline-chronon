from group_bys.aws import dayoffset_gridded, dayoffset_inherited

from ai.chronon.types import EventSource, Join, JoinPart, Query, selects

"""
Scaled-down stand-in for a 1d@1h join with both 3h@1h and 1d@1h group-by parts.
The source timestamps are raw epoch milliseconds and are quantized onto each
consumer's grid through time_partitioned dependencies.
"""

source = EventSource(
    table="demo.user_activities_raw",
    query=Query(
        selects=selects(
            listing_id="listing_id",
            user_id="user_id",
            event_id="event_id",
        ),
        time_column="event_time_ms",
        partition_column="event_time_ms",
        time_partitioned=True,
    ),
)

v10 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=dayoffset_gridded.v8, prefix="gridded_"),
        JoinPart(group_by=dayoffset_inherited.v8, prefix="inherited_"),
    ],
    version=10,
    online=True,
    output_namespace="data",
    step_days=30,
    partition_interval="1d",
    partition_offset="1h",
    offline_schedule="30 2 * * *",
    online_schedule="30 2 * * *",
)
