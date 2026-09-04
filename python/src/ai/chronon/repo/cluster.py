import json

# GCP initialization scripts that should be included in all Dataproc clusters
# These paths are relative to the artifact_prefix/release/{version} directory
GCP_DEFAULT_INIT_SCRIPTS = [
    "/scripts/gcp/copy_java_security.sh",
    "/scripts/gcp/opsagent_setup.sh",
]


def generate_emr_cluster_config(
    instance_count,
    subnet_name,
    security_group_name,
    instance_type="m5.xlarge",
    idle_timeout=3600,
    release_label="emr-7.12.0",
):
    """
    Create a configuration for an EMR cluster.
    :param instance_count: Number of instances in the cluster
    :param subnet_name: Name tag of the subnet (will be looked up to get subnet ID)
    :param security_group_name: Name of the security group (will be looked up)
    :param instance_type: EC2 instance type for cluster nodes
    :param idle_timeout: Idle timeout in seconds before cluster auto-terminates
    :param release_label: EMR release label version
    :return: A json string representing the configuration.
    """
    config = {
        "releaseLabel": release_label,
        "instanceType": instance_type,
        "instanceCount": instance_count,
        "autoTerminationPolicy": {"idleTimeout": idle_timeout},
    }

    config["subnetName"] = subnet_name
    config["securityGroupName"] = security_group_name

    return json.dumps(config)


def generate_dataproc_cluster_config(
    num_workers,
    project_id,
    artifact_prefix,
    version="latest", # TODO: we should enforce this though.
    master_host_type="n2-highmem-64",
    worker_host_type="n2-highmem-16",
    subnetwork="default",
    idle_timeout="7200s",
    master_boot_disk_size_gb=1024,
    worker_boot_disk_size_gb=64,
    worker_local_ssds=2,
    initialization_actions=None,
    tags=None,
):
    """
    Create a configuration for a Dataproc cluster.
    :return: A json string representing the configuration.
    """
    if initialization_actions is None:
        initialization_actions = []
    return json.dumps(
        {
            "gceClusterConfig": {
                "subnetworkUri": subnetwork,
                "serviceAccount": "dataproc@" + project_id + ".iam.gserviceaccount.com",
                "serviceAccountScopes": [
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/monitoring",
                    "https://www.googleapis.com/auth/cloud.useraccounts.readonly",
                    "https://www.googleapis.com/auth/devstorage.read_write",
                    "https://www.googleapis.com/auth/logging.write",
                ],
                "metadata": {
                    "hive-version": "3.1.2",
                    "SPARK_BQ_CONNECTOR_URL": "gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.1.jar",
                    "artifact_prefix": artifact_prefix.rstrip("/"),
                },
                "tags": tags or [],
            },
            "masterConfig": {
                "numInstances": 1,
                "machineTypeUri": master_host_type,
                "diskConfig": {
                    "bootDiskType": "pd-standard",
                    "bootDiskSizeGb": master_boot_disk_size_gb,
                },
            },
            "workerConfig": {
                "numInstances": num_workers,
                "machineTypeUri": worker_host_type,
                "diskConfig": {
                    "bootDiskType": "pd-standard",
                    "bootDiskSizeGb": worker_boot_disk_size_gb,
                    "numLocalSsds": worker_local_ssds,
                },
            },
            "softwareConfig": {
                "imageVersion": "2.2.66-debian12",
                "optionalComponents": [
                    "FLINK",
                    "JUPYTER",
                ],
                "properties": {
                    "dataproc:dataproc.logging.stackdriver.enable": "true",
                    "dataproc:jobs.file-backed-output.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                    "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
                },
            },
            "initializationActions": [
                {"executable_file": initialization_action}
                for initialization_action in (
                    (initialization_actions or [])
                    + [("/".join([artifact_prefix.rstrip("/"), f"release/{version}"])) + s for s in GCP_DEFAULT_INIT_SCRIPTS]
                )
            ],
            "endpointConfig": {
                "enableHttpPortAccess": True,
            },
            "lifecycleConfig": {
                "idleDeleteTtl": idle_timeout,
            },
        }
    )


def fixed_gcp_cluster(
    size,
    project_id,
    artifact_prefix,
    subnetwork="default",
    initialization_actions=None,
    tags=None,
):
    """
    Create a Dataproc cluster configuration based on t-shirt sizes.

    :param size: T-shirt size - 'small', 'medium', or 'large'
    :param project_id: GCP project ID
    :param artifact_prefix: Artifact prefix for initialization scripts
    :param subnetwork: Subnetwork for the cluster
    :param initialization_actions: List of initialization actions
    :param tags: List of tags for the cluster
    :return: A json string representing the cluster configuration
    """
    size_configs = {
        "small": {
            "num_workers": 20,
            "worker_host_type": "n2-highmem-4",  # 16GB, 4 cores
            "master_host_type": "n2-highmem-4",  # Same as worker for consistency
        },
        "medium": {
            "num_workers": 50,
            "worker_host_type": "n2-highmem-16",  # 32GB, 8 cores
            "master_host_type": "n2-highmem-16",  # Same as worker for consistency
        },
        "large": {
            "num_workers": 250,
            "worker_host_type": "n2-highmem-16",  # 64GB, 16 cores
            "master_host_type": "n2-highmem-16",  # Same as worker for consistency
        },
    }

    if size not in size_configs:
        raise ValueError(f"Invalid size '{size}'. Must be one of: {list(size_configs.keys())}")

    config = size_configs[size]

    return generate_dataproc_cluster_config(
        num_workers=config["num_workers"],
        project_id=project_id,
        artifact_prefix=artifact_prefix,
        master_host_type=config["master_host_type"],
        worker_host_type=config["worker_host_type"],
        subnetwork=subnetwork,
        idle_timeout="3600s",  # 1 hour of inactivity
        initialization_actions=initialization_actions,
        tags=tags,
    )
