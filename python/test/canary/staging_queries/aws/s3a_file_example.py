from ai.chronon.types import EngineType, StagingQuery

# This object is a CI-owned fixture outside the Iceberg-managed data/table paths.
# Keep it stable: the AWS integration test depends on it for S3A file access.
PARQUET_URI = (
    "s3a://zipline-warehouse-canary/integration-test-fixtures/"
    "s3a-file-example/dim-listings.parquet"
)

setup = f"""
    CREATE OR REPLACE TEMPORARY VIEW s3a_file_example
    USING parquet
    OPTIONS (path '{PARQUET_URI}')
"""

v1 = StagingQuery(
    query="""
        SELECT *
        FROM s3a_file_example
    """,
    setups=[setup],
    output_namespace="data",
    engine_type=EngineType.SPARK,
    dependencies=[],
)
