from ai.chronon.types import ConfigProperties, EngineType, StagingQuery

SCALE_ROWS = 2_000_000
SHUFFLE_PARTITIONS = 1_024

_payload_parts = [
    f"SHA2(CONCAT_WS('|', CAST(base.listing_id AS STRING), CAST(scale.id AS STRING), '{salt}'), 256)"
    for salt in range(8)
]

scaled_dim_listings = StagingQuery(
    query=f"""
    SELECT
        CONCAT(CAST(base.listing_id AS STRING), '-', CAST(scale.id AS STRING)) AS stress_row_id,
        base.listing_id,
        base.merchant_id,
        base.price_cents,
        PMOD(XXHASH64(base.listing_id, scale.id), {SHUFFLE_PARTITIONS}) AS shuffle_key,
        CONCAT({", ".join(_payload_parts)}) AS stress_payload,
        base.ds
    FROM workspace.poc.dim_listings AS base
    CROSS JOIN range(0, {SCALE_ROWS}, 1, {SHUFFLE_PARTITIONS}) AS scale
    WHERE base.ds BETWEEN {{{{ start_date }}}} AND {{{{ end_date }}}}
    DISTRIBUTE BY shuffle_key
    SORT BY shuffle_key, stress_row_id
    """,
    output_namespace="zipline_catalog.default",
    engine_type=EngineType.SPARK,
    # This is an intentionally synthetic load test; dependency sensors would
    # reject the sparse fixture range before Spark can generate the scaled rows.
    dependencies=[],
    version=0,
    offline_schedule="@never",
    environments=["canary"],
    step_days=10_000,
    conf=ConfigProperties(
        common={
            "spark.executor.cores": "4",
            "spark.executor.memory": "2g",
            "spark.executor.instances": "1",
            "spark.dynamicAllocation.enabled": "false",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.initialExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "1",
            "spark.default.parallelism": str(SHUFFLE_PARTITIONS),
            "spark.sql.shuffle.partitions": str(SHUFFLE_PARTITIONS),
            "spark.sql.adaptive.enabled": "false",
        }
    ),
    tags={
        "purpose": "nvme-stress",
        "rows_per_input_row": str(SCALE_ROWS),
        "payload_bytes_per_row": "512",
    },
)
