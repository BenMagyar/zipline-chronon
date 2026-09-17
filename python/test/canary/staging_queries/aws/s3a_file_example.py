from ai.chronon.types import EngineType, StagingQuery


PARQUET_URI = (
    "s3a://zipline-warehouse/data/tables/demo/dim_listings/data/"
    "ds=2026-09-17/00000-0-f9a51a3f-a9b4-457e-9895-f40daa553b00.parquet"
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
