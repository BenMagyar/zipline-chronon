from group_bys.aws_databricks import dim_listings, user_activities
from staging_queries.aws_databricks import exports

from ai.chronon.types import Derivation, EntitySource, EventSource, Join, JoinPart, Query, selects

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

# Mirrors joins/aws/demo.derivations_v1 on the aws_databricks stack: same left
# source (user_activities events) and right parts (dim_listings + user_activities),
# with derivations exercising Spark expressions over the joined listing columns.
# Uses the same workspace.demo.user_activities table as adhoc_v1 below to keep
# both event-source joins pointing at a single Kinesis-fed table.
derivations_v1 = Join(
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
        JoinPart(group_by=dim_listings.adhoc_v1),
        JoinPart(group_by=user_activities.v1),
    ],
    derivations=[
        Derivation(
            name="is_listing_heavy",
            expression="IF(listing_id_weight_grams > 1000, 1, 0)",
        ),
        Derivation(
            name="is_item_handmade",
            expression="array_contains(split(listing_id_tags, ','), 'handmade')",
        ),
        Derivation(name="price_log", expression="log1p(listing_id_price_cents)"),
        Derivation(
            name="price_bucket",
            expression="""
                CASE
                    WHEN listing_id_price_cents < 1000 THEN 0
                    WHEN listing_id_price_cents < 5000 THEN 1
                    WHEN listing_id_price_cents < 10000 THEN 2
                    WHEN listing_id_price_cents < 50000 THEN 3
                    ELSE 4
                END
            """,
        ),
        Derivation(name="inventory_log", expression="log1p(listing_id_inventory_count)"),
        Derivation(
            name="price_per_kg",
            expression="listing_id_price_cents * 1000.0 / NULLIF(listing_id_weight_grams, 0)",
        ),
        Derivation(name="*", expression="*"),
    ],
    version=3,
    online=False,
    output_namespace="workspace_iceberg.poc",
    step_days=10,
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
