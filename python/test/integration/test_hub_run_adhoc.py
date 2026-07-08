"""Cloud-agnostic integration tests for ``zipline hub run-adhoc`` and ``zipline hub cancel``.

Replaces the former test_gcp_run_adhoc.py.
"""

import logging
import os

import pytest
from click.testing import CliRunner

from .helpers.cleanup import DataprocFlinkCleanup
from .helpers.cli import cancel_workflow, compile_configs, submit_run_adhoc
from .helpers.hub_api import cancel_streaming_node, get_continuous_node_names, get_flink_job_ids
from .helpers.workflow import poll_workflow_until

logger = logging.getLogger(__name__)

# Key: "{cloud}_{compile_env}" where compile_env is ZIPLINE_COMPILE_ENV (default "prod").
# Value: (conf_path, end_ds)
ADHOC_CONFIGS = {
    "gcp_prod":   ("compiled/joins/gcp/demo.v1__1",                          "2025-08-01"),
    "aws_prod":   ("compiled/joins/aws/demo.v1__1",                          "2025-08-01"),
    "aws_canary": ("compiled_canary/joins/aws_databricks/demo.adhoc_v1__1",  "2026-06-30"),
    "azure_prod": ("compiled/joins/azure/demo.v2",                           "2025-08-01"),
}


def _adhoc_key(cloud: str) -> str:
    env = os.environ.get("ZIPLINE_COMPILE_ENV", "prod") or "prod"
    return f"{cloud}_{env}"


@pytest.fixture
def flink_cleanup(hub_url, cloud):
    """Cancel streaming Flink jobs after the test.

    For Hub-managed streaming (e.g. aws_canary): fetches continuous node names
    from the workflow plan and cancels them via the Hub /streaming/v1/cancel endpoint.
    For Dataproc-managed streaming (e.g. gcp): fetches Dataproc job IDs from the
    workflow status and cancels them directly.
    """
    ctx = {}
    yield ctx
    if "workflow_id" not in ctx:
        return

    workflow_id = ctx["workflow_id"]

    if _adhoc_key(cloud) == "aws_canary":
        node_names = get_continuous_node_names(hub_url, workflow_id)
        assert node_names, (
            f"flink_cleanup: no continuous nodes found in workflow {workflow_id}. "
            "A streaming Flink job may still be running — check and cancel manually."
        )
        for node_name in node_names:
            try:
                cancel_streaming_node(hub_url, node_name)
            except Exception as exc:
                logger.warning("flink_cleanup: Hub cancel failed for %s: %s", node_name, exc)
        return

    job_ids = get_flink_job_ids(hub_url, workflow_id)
    assert job_ids, (
        f"flink_cleanup: no Flink job IDs found for workflow {workflow_id}. "
        "A streaming Flink job may still be running on Dataproc — check and cancel manually."
    )
    project = os.environ.get("GCP_PROJECT_ID", "canary-443022")
    region = os.environ.get("GCP_REGION", "us-central1")
    DataprocFlinkCleanup(project, region).cancel_jobs(job_ids)


@pytest.mark.integration
def test_run_adhoc(confs, chronon_root, hub_url, cloud, flink_cleanup):
    """run-adhoc launches a streaming deploy and waits for SUCCEEDED."""
    config = ADHOC_CONFIGS.get(_adhoc_key(cloud))
    if not config:
        pytest.skip(f"skipping test_run_adhoc for cloud={cloud} env={_adhoc_key(cloud)}")
    conf_path, end_ds = config
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs(conf_path), end_ds,
    )
    flink_cleanup["workflow_id"] = workflow_id

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"SUCCEEDED"}, timeout=1200, interval=15,
    )


@pytest.mark.integration
def test_run_adhoc_no_data(confs, chronon_root, hub_url, cloud):
    """run-adhoc with dates that have no input data should fail."""
    config = ADHOC_CONFIGS.get(_adhoc_key(cloud))
    if not config:
        pytest.skip(f"skipping test_run_adhoc_no_data for cloud={cloud} env={_adhoc_key(cloud)}")
    conf_path, _ = config
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_run_adhoc(
        runner, chronon_root, hub_url,
        confs(conf_path), "1969-01-01",
    )

    poll_workflow_until(
        hub_url, workflow_id, target_statuses={"FAILED"}, timeout=1800, interval=30,
    )
