from ai.chronon.types import Aggregation, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects

"""
A temporal event group_by whose OUTPUT lands on 3h@1h, uploaded online.

Mirrors the customer's `ranking_common_hydrated_interactions_*` readers -- roughly a dozen
of their parts sit on a 3h@1h grid under the 1d@1h join. A temporal part under a grid-aware
join must either declare an interval or be time_partitioned (PartitionSpecResolver rejects
it otherwise); here the source is time_partitioned and the sub-daily grid is declared on the
output, which is what the timeline actually renders.
"""

source = EventSource(
    table="demo.user_activities_raw",
    query=Query(
        selects=selects(
            listing_id="listing_id",
            carted="IF(event_type = 'add_to_cart', 1, 0)",
        ),
        time_column="event_time_ms",
        partition_column="event_time_ms",
        # Source is plain daily; declaring that truthfully trips the alignment check against
        # a 1d@1h consumer, so it stays time_partitioned-only (readiness from timestamps).
        # This is the shape the hub accepts.
        time_partitioned=True,
    ),
)

v8 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    aggregations=[
        Aggregation(input_column="carted", operation=Operation.SUM, windows=[Window(7, TimeUnit.DAYS)])
    ],
    version=8,
    online=True,
    output_namespace="data",
    # 3h@1h output. This is the row that renders as hair-thin slivers on a 90-day axis
    # (a 3h box is 1/240th of a 30-day span) wherever adjacent partitions differ in status,
    # and as one fat block wherever they agree and get run-length-encoded together --
    # the customer's `hydrated_interactions.added_to_bag v4` row.
    partition_interval="3h",
    partition_offset="1h",
    offline_schedule="0 1/3 * * *",
    online_schedule="0 1/3 * * *",
)
