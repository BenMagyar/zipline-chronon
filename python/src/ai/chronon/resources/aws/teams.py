from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.table_write.format": "iceberg",
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",

            "spark.chronon.table_write.upload.format": "ion",
            # Flink
            "taskmanager.memory.process.size": "16G",

            # TODO: Choose a data catalog configuration.

            # TODO: Please fill in the following values
            "spark.chronon.table_write.upload.location": "s3://zipline-warehouse-<customer_id>/data/ion_uploads/",
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            # TODO: Please fill in the following values
            "CUSTOMER_ID": "<customer_id>",
            "AWS_REGION": "<region>",
            "ARTIFACT_PREFIX": "s3://zipline-artifacts-<customer_id>",
            "WAREHOUSE_PREFIX": "s3://zipline-warehouse-<customer_id>",
            "CLOUD_PROVIDER": "aws",
            "HUB_URL": "<hub-url>",  # URL to the Zipline Hub control plane
            "FRONTEND_URL": "<frontend-url>",  # URL to the Zipline Frontend
        },
    ),
)


test = Team(
    outputNamespace="data",
    env=EnvironmentVariables(
        common={}, modeEnvironments={RunMode.BACKFILL: {}, RunMode.UPLOAD: {}}
    ),
)

team_conf = Team(
    outputNamespace="test",
    env=EnvironmentVariables(
        common={},
    ),
)
