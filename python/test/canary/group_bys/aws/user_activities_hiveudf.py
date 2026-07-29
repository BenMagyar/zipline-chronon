from ai.chronon.types import Aggregation, ConfigProperties, EventSource, GroupBy, Operation, Query, TimeUnit, Window, selects
from ai.chronon.types import EnvironmentVariables

"""
Fork of user_activities.py that exercises Hive UDF support in Flink

setups on the Query loads a Hive UDF jar from S3 via ADD JAR, then registers MINUS_ONE via
CREATE FUNCTION. 
"""

_setups = [
    "ADD JAR 's3a://zipline-warehouse-canary/hive-udfs/chronon-test-udfs.jar'",
    "CREATE TEMPORARY FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'",
]

source = EventSource(
    table="demo.user_activities_raw",
    topic="kinesis://user-activities/serde=glue_registry/registry_name=zipline-canary/schema_name=user-activities",
    query=Query(
        selects=selects(
            user_id="user_id",
            listing_id="listing_id",
            view_event="IF(event_type = 'view', 1, 0)",
            click_event="IF(event_type = 'click', 1, 0)",
            purchase_event="IF(event_type = 'purchase', 1, 0)",
            favorite_event="IF(event_type = 'favorite', 1, 0)",
            add_to_cart_event="IF(event_type = 'add_to_cart', 1, 0)",
            is_mobile="IF(device_type = 'mobile', 1, 0)",
            is_desktop="IF(device_type = 'desktop', 1, 0)",
            is_tablet="IF(device_type = 'tablet', 1, 0)",
            user_event_struct="STRUCT(event_type, listing_id, unix_millis(TIMESTAMP(event_time_ms)) as timestamp)",
            # Cast ingested_time_ms (long) to INT before passing to the Hive UDF
            ingested_time_int_minus_1="MINUS_ONE(CAST(ingested_time_ms AS INT))",
        ),
        time_column="unix_millis(TIMESTAMP(event_time_ms))",
        setups=_setups,
    ),
)

window_sizes = [Window(length=days, time_unit=TimeUnit.DAYS) for days in [1, 7, 14, 30]]

event_columns = ["view_event", "click_event", "purchase_event", "favorite_event", "add_to_cart_event"]
device_columns = ["is_mobile", "is_desktop", "is_tablet"]
last_k_columns = ["user_event_struct"]

aggregations = []

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.AVERAGE, windows=window_sizes)
    for col in event_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.SUM, windows=window_sizes)
    for col in device_columns
])

aggregations.extend([
    Aggregation(input_column=col, operation=Operation.LAST_K(128), windows=window_sizes)
    for col in last_k_columns
])

aggregations.append(
    Aggregation(input_column="ingested_time_int_minus_1", operation=Operation.SUM, windows=window_sizes)
)

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=False,
    version=1,
    aggregations=aggregations,
    step_days=10,
    # GlueConfiguration sets defaultUrlStreamHandlerFactory.enabled=false globally to avoid
    # conflicts with the Iceberg/Glue catalog setup. GroupBys that use ADD JAR with cloud URIs
    # (s3a://, gs://) need it re-enabled so java.net.URL can resolve those schemes via
    # Hadoop's FsUrlStreamHandlerFactory. This is a startup property so it takes effect
    # before session init even though it's a static config.
    conf=ConfigProperties(
        common={
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
    ),
    env_vars=EnvironmentVariables(
        common={
            "CHRONON_ONLINE_ARGS": "-Ztasks=1",
        }
    ),
)
