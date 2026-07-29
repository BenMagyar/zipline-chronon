from group_bys.aws import dim_listings, dim_merchants, user_activities_hiveudf
from staging_queries.aws import exports

from ai.chronon.types import (
    ConfigProperties,
    Derivation,
    EventSource,
    Join,
    JoinPart,
    Query,
    selects,
)

"""
Demo join that exercises Hive UDF support.
"""

_setups = [
    "ADD JAR 's3a://zipline-artifacts-canary/hive-udfs/chronon-test-udfs.jar'",
    "CREATE TEMPORARY FUNCTION MINUS_ONE AS 'ai.chronon.online.test.Minus_One'",
]

source = EventSource(
    table=exports.user_activities.table,
    query=Query(
        selects=selects(user_id="user_id", listing_id="listing_id", row_id="event_id"),
        time_column="event_time_ms",
        # Setups are executed by PooledCatalystUtil before any SQL expressions are evaluated,
        # including derivations — so the UDF registered here is available at derivation time.
        setups=_setups,
    ),
)

v1 = Join(
    left=source,
    row_ids=["event_id"],
    right_parts=[
        JoinPart(group_by=user_activities_hiveudf.v1),
        JoinPart(group_by=dim_listings.v1),
        JoinPart(group_by=dim_merchants.v1, prefix="merchant_"),
    ],
    derivations=[
        # Invoke the Hive UDF on listing_id as a derived feature.
        Derivation(name="listing_id_minus_1", expression="MINUS_ONE(CAST(listing_id AS INT))"),
        Derivation(name="*", expression="*"),
    ],
    version=1,
    online=False,
    output_namespace="data",
    # Enable the URL stream handler factory so ADD JAR s3a:// works.
    # GlueConfiguration sets this to false globally; override here since this
    # join uses Hive UDF jars loaded from S3.
    conf=ConfigProperties(
        common={
            "spark.sql.defaultUrlStreamHandlerFactory.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }
    ),
    step_days=10,
)
