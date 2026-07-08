from staging_queries.aws_databricks import exports

from ai.chronon.types import EntitySource, GroupBy, Query, selects

source = EntitySource(
    snapshot_table=exports.dim_listings.table,
    query=Query(
        selects=selects(
            listing_id="CAST(listing_id AS INT)",
            merchant_id="merchant_id",
            headline="headline",
            brief_description="brief_description",
            long_description="long_description",
            price_cents="price_cents",
            currency="currency",
            inventory_count="inventory_count",
            primary_category="primary_category",
            is_active="is_active",
            weight_grams="weight_grams",
            tags="tags",
            is_expensive="IF(price_cents > 10000, 1, 0)",
            is_in_stock="IF(inventory_count > 0, 1, 0)",
            main_image_path="main_image_path",
            secondary_image_paths="secondary_image_paths",
        ),
        start_partition="2025-01-01",
    ),
)

v1 = GroupBy(
    sources=[source],
    keys=["listing_id"],
    online=False,
    aggregations=None,
    output_namespace="workspace_iceberg.poc",
)

# Smaller select set used by the four partitioned/unpartitioned × dense/sparse
# canary variants below. Keeps backfill payload tight when only listing_id is
# joined on the left side.
_pt_selects = selects(
    listing_id="listing_id",
    merchant_id="merchant_id",
    headline="headline",
    price_cents="price_cents",
    currency="currency",
    is_active="is_active",
)

pt_v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table=exports.dim_listings_pt.table,
        query=Query(selects=_pt_selects, start_partition="2025-01-01"),
    )],
    keys=["listing_id"],
    online=False,
    version=0,
    aggregations=None,
    step_days=30,
)

sparse_v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table=exports.dim_listings_sparse.table,
        query=Query(selects=_pt_selects, start_partition="2025-01-01"),
    )],
    keys=["listing_id"],
    online=False,
    version=0,
    aggregations=None,
    step_days=30,
)

unpartitioned_v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned.table,
        query=Query(selects=_pt_selects, start_partition="2025-01-01"),
    )],
    keys=["listing_id"],
    online=False,
    version=0,
    aggregations=None,
    step_days=30,
)

unpartitioned_sparse_v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table=exports.dim_listings_unpartitioned_sparse.table,
        query=Query(selects=_pt_selects, start_partition="2025-01-01"),
    )],
    keys=["listing_id"],
    online=False,
    version=0,
    aggregations=None,
    step_days=30,
)

# Points directly at workspace.demo.dim_listings — the same location used for run-adhoc tests.
adhoc_v1 = GroupBy(
    sources=[EntitySource(
        snapshot_table="workspace.demo.dim_listings",
        query=Query(
            selects=selects(
                listing_id="CAST(listing_id AS INT)",
                merchant_id="merchant_id",
                headline="headline",
                brief_description="brief_description",
                long_description="long_description",
                price_cents="price_cents",
                currency="currency",
                inventory_count="inventory_count",
                primary_category="primary_category",
                is_active="is_active",
                weight_grams="weight_grams",
                tags="tags",
                is_expensive="IF(price_cents > 10000, 1, 0)",
                is_in_stock="IF(inventory_count > 0, 1, 0)",
                main_image_path="main_image_path",
                secondary_image_paths="secondary_image_paths",
            ),
        ),
    )],
    keys=["listing_id"],
    online=False,
    aggregations=None,
    output_namespace="workspace_iceberg.poc",
)
