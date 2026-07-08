from group_bys.aws_databricks import dim_listings, user_activities
from staging_queries.aws_databricks import exports

from ai.chronon.types import EntitySource, EventSource, Join, JoinPart, Query, selects

v1 = Join(
    left=dim_listings.source,
    row_ids=[],
    right_parts=[
        JoinPart(
            group_by=dim_listings.v1,
        ),
    ],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
    enable_stats_compute=True,
)

# Four partitioned/unpartitioned × dense/sparse variants exercised by
# test_hub_backfill_uc.py against the Crucible-backed AWS Hub.
_left_selects = selects(listing_id="listing_id")

pt_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_pt.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.pt_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

sparse_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_sparse.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.sparse_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

unpartitioned_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.unpartitioned_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

unpartitioned_sparse_v1 = Join(
    left=EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned_sparse.table,
        query=Query(selects=_left_selects, start_partition="2025-01-01"),
    ),
    row_ids=[],
    right_parts=[JoinPart(group_by=dim_listings.unpartitioned_sparse_v1)],
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=30,
)

# Event-source join for run-adhoc streaming integration tests.
# Left: user activity events from Kinesis; right: user behavioral aggregations + listing attributes.
adhoc_v1 = Join(
    left=EventSource(
        table="workspace.demo.user_activities",
        query=Query(
            selects=selects(
                user_id="user_id",
                listing_id="listing_id",
                row_id="event_id",
            ),
            time_column="event_time_ms",
        ),
    ),
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=user_activities.v1),
        JoinPart(group_by=dim_listings.adhoc_v1),
    ],
    version=1,
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=10,
)
