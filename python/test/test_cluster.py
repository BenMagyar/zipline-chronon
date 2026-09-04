import json
import os

import pytest

from ai.chronon.repo.cluster import (
    GCP_DEFAULT_INIT_SCRIPTS,
    fixed_gcp_cluster,
    generate_dataproc_cluster_config,
    generate_emr_cluster_config,
)


class TestDataprocClusterConfig:
    """Tests for Dataproc cluster configuration generation."""

    def test_basic_dataproc_config(self):
        """Verify basic Dataproc cluster configuration structure."""
        config_json = generate_dataproc_cluster_config(
            num_workers=10,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.0.0",
        )
        config = json.loads(config_json)

        assert "gceClusterConfig" in config
        assert "masterConfig" in config
        assert "workerConfig" in config
        assert "softwareConfig" in config
        assert "initializationActions" in config
        assert config["workerConfig"]["numInstances"] == 10
        assert config["masterConfig"]["numInstances"] == 1

    def test_custom_disk_config(self):
        """Verify callers can right-size Dataproc boot and local disks."""
        config_json = generate_dataproc_cluster_config(
            num_workers=2,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            master_boot_disk_size_gb=100,
            worker_boot_disk_size_gb=64,
            worker_local_ssds=0,
        )
        config = json.loads(config_json)

        assert config["masterConfig"]["diskConfig"] == {
            "bootDiskType": "pd-standard",
            "bootDiskSizeGb": 100,
        }
        assert config["workerConfig"]["diskConfig"] == {
            "bootDiskType": "pd-standard",
            "bootDiskSizeGb": 64,
            "numLocalSsds": 0,
        }

    def test_initialization_scripts_are_included(self):
        """Verify default GCP initialization scripts are included in the config."""
        config_json = generate_dataproc_cluster_config(
            num_workers=5,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.2.3",
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        # Verify all default scripts are present
        for script in GCP_DEFAULT_INIT_SCRIPTS:
            expected_path = f"gs://test-bucket/artifacts/release/1.2.3{script}"
            assert expected_path in executable_files, f"Missing required script: {script}"

    def test_custom_initialization_actions(self):
        """Verify custom initialization actions are added to default scripts."""
        custom_scripts = [
            "gs://custom-bucket/custom-script1.sh",
            "gs://custom-bucket/custom-script2.sh",
        ]

        config_json = generate_dataproc_cluster_config(
            num_workers=5,
            project_id="test-project",
            artifact_prefix="gs://test-bucket/artifacts",
            version="1.0.0",
            initialization_actions=custom_scripts,
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        # Verify custom scripts are included
        for script in custom_scripts:
            assert script in executable_files

        # Verify default scripts are still included
        for script in GCP_DEFAULT_INIT_SCRIPTS:
            expected_path = f"gs://test-bucket/artifacts/release/1.0.0{script}"
            assert expected_path in executable_files

    def test_artifact_prefix_trailing_slash_handling(self):
        """Verify artifact prefix handles trailing slashes correctly."""
        configs = []
        for prefix in ["gs://bucket/path", "gs://bucket/path/"]:
            config_json = generate_dataproc_cluster_config(
                num_workers=1,
                project_id="test-project",
                artifact_prefix=prefix,
                version="1.0.0",
            )
            configs.append(json.loads(config_json))

        # Both configs should produce identical script paths
        init_files_1 = [a["executable_file"] for a in configs[0]["initializationActions"]]
        init_files_2 = [a["executable_file"] for a in configs[1]["initializationActions"]]
        assert init_files_1 == init_files_2

    def test_version_in_script_paths(self):
        """Verify version is correctly embedded in script paths."""
        version = "2.5.10"
        config_json = generate_dataproc_cluster_config(
            num_workers=1,
            project_id="test-project",
            artifact_prefix="gs://bucket",
            version=version,
        )
        config = json.loads(config_json)

        init_actions = config["initializationActions"]
        executable_files = [action["executable_file"] for action in init_actions]

        for script in GCP_DEFAULT_INIT_SCRIPTS:
            expected_path = f"gs://bucket/release/{version}{script}"
            assert expected_path in executable_files


class TestScriptValidation:
    """Tests to validate that required GCP scripts exist in the repository."""

    def test_gcp_default_scripts_exist_in_repo(self):
        """Verify all default GCP init scripts actually exist in scripts/gcp/."""
        # Get the repo root (assuming test is in python/test/)
        test_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(test_dir))
        scripts_dir = os.path.join(repo_root, "scripts", "gcp")

        for script_path in GCP_DEFAULT_INIT_SCRIPTS:
            # Extract filename from path like "/scripts/gcp/opsagent_setup.sh"
            script_name = os.path.basename(script_path)
            full_path = os.path.join(scripts_dir, script_name)

            assert os.path.isfile(full_path), (
                f"Required GCP initialization script not found: {script_name}\n"
                f"Expected at: {full_path}\n"
                f"Update GCP_DEFAULT_INIT_SCRIPTS in cluster.py if this script was renamed or removed."
            )

    def test_gcp_scripts_directory_exists(self):
        """Verify scripts/gcp directory exists."""
        test_dir = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(os.path.dirname(test_dir))
        scripts_dir = os.path.join(repo_root, "scripts", "gcp")

        assert os.path.isdir(scripts_dir), f"GCP scripts directory not found: {scripts_dir}"


class TestFixedGcpCluster:
    """Tests for fixed_gcp_cluster t-shirt sizing function."""

    def test_small_cluster_config(self):
        """Verify small cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="small",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 20
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-4"

    def test_medium_cluster_config(self):
        """Verify medium cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="medium",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 50
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-16"

    def test_large_cluster_config(self):
        """Verify large cluster configuration."""
        config_json = fixed_gcp_cluster(
            size="large",
            project_id="test-project",
            artifact_prefix="gs://test-bucket",
        )
        config = json.loads(config_json)

        assert config["workerConfig"]["numInstances"] == 250
        assert config["workerConfig"]["machineTypeUri"] == "n2-highmem-16"

    def test_invalid_size_raises_error(self):
        """Verify invalid size raises ValueError."""
        with pytest.raises(ValueError, match="Invalid size"):
            fixed_gcp_cluster(
                size="xlarge",  # Invalid size
                project_id="test-project",
                artifact_prefix="gs://test-bucket",
            )


class TestEmrClusterConfig:
    """Tests for EMR cluster configuration generation."""

    def test_basic_emr_config(self):
        """Verify basic EMR cluster configuration structure."""
        config_json = generate_emr_cluster_config(
            instance_count=10,
            subnet_name="test-subnet",
            security_group_name="test-sg",
        )
        config = json.loads(config_json)

        assert config["instanceCount"] == 10
        assert config["subnetName"] == "test-subnet"
        assert config["securityGroupName"] == "test-sg"
        assert "releaseLabel" in config
        assert "instanceType" in config
        assert "autoTerminationPolicy" in config

    def test_emr_custom_instance_type(self):
        """Verify custom instance type is used."""
        config_json = generate_emr_cluster_config(
            instance_count=5,
            subnet_name="test-subnet",
            security_group_name="test-sg",
            instance_type="r5.2xlarge",
        )
        config = json.loads(config_json)

        assert config["instanceType"] == "r5.2xlarge"
