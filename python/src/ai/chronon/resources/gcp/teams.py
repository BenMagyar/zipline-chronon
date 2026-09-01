from ai.chronon.repo.constants import RunMode
from ai.chronon.types import ConfigProperties, EnvironmentVariables, Team

default = Team(
    description="Default team",
    email="<responsible-team-email>",
    outputNamespace="default",
    conf=ConfigProperties(
        common={
            "spark.chronon.coalesce.factor": "10",
            "spark.default.parallelism": "10",
            "spark.sql.shuffle.partitions": "10",

            "spark.chronon.table.format_provider.class": "ai.chronon.integrations.cloud_gcp.GcpFormatProvider",

            # TODO: Choose a data catalog configuration.

            # TODO: Please fill in the following values
            "spark.chronon.partition.format": "<date-format>",  # ex: "yyyy-MM-dd",
            "spark.chronon.partition.column": "<partition-column-name>",  # ex: "ds",
        },
    ),
    env=EnvironmentVariables(
        common={
            # TODO: Please fill in the following values
            "CUSTOMER_ID": "<customer_id>",
            "GCP_PROJECT_ID": "<project-id>",
            "GCP_REGION": "<region>",
            "GCP_DATAPROC_CLUSTER_NAME": "<dataproc-cluster-name>",
            "GCP_BIGTABLE_INSTANCE_ID": "<bigtable-instance-id>",
            "ARTIFACT_PREFIX": "<customer-artifact-bucket>",
            "WAREHOUSE_PREFIX": "gs://zipline-warehouse-<customer_id>",
            "CLOUD_PROVIDER": "<gcp | aws>",
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
