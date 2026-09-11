from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects

"""
A group_by whose source declares no physical partition grid and sets
time_partitioned=True, so it is quantized on the consuming node's grid.
"""

source = EventSource(
    table="demo.user_activities_raw",
    query=Query(
        selects=selects(
            listing_id="listing_id",
            viewed="IF(event_type = 'view', 1, 0)",
        ),
        time_column="event_time_ms",
        partition_column="event_time_ms",
        time_partitioned=True,
    ),
)

v8 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    aggregations=[
        Aggregation(
            input_column="viewed",
            operation=Operation.SUM,
            windows=[Window(7, TimeUnit.DAYS)],
        )
    ],
    version=8,
    online=True,
    output_namespace="data",
    partition_interval="1d",
    partition_offset="1h",
    offline_schedule="30 2 * * *",
    online_schedule="30 2 * * *",
)
