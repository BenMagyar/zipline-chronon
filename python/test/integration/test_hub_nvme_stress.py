"""AWS Crucible integration coverage for executor-local NVMe spill."""

import json
import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

_COMPILE_ENV = os.environ.get("ZIPLINE_COMPILE_ENV", "prod").strip() or "prod"
_COMPILED_DIR = "compiled" if _COMPILE_ENV == "prod" else f"compiled_{_COMPILE_ENV}"

NVME_STRESS_CONF = (
    f"{_COMPILED_DIR}/staging_queries/aws_databricks/nvme_stress.scaled_dim_listings__0"
)
START_DS = "2024-12-31"
END_DS = "2026-02-28"

EXPECTED_SPARK_CONF = {
    "spark.driver.cores": "1",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "4",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "1",
    "spark.dynamicAllocation.enabled": "false",
    "spark.default.parallelism": "1024",
    "spark.sql.shuffle.partitions": "1024",
    "spark.sql.adaptive.enabled": "false",
}


@pytest.mark.integration
def test_backfill_nvme_stress(confs, chronon_root, hub_url, cloud):
    """Force a large single-executor shuffle through Crucible's local storage."""
    if cloud != "aws":
        pytest.skip(f"NVMe stress backfill is AWS-only; cloud={cloud}")
    if _COMPILE_ENV != "canary":
        pytest.skip("NVMe stress backfill requires ZIPLINE_COMPILE_ENV=canary")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    conf_path = confs(NVME_STRESS_CONF)
    with open(os.path.join(chronon_root, conf_path)) as conf_file:
        compiled_conf = json.load(conf_file)

    execution_info = compiled_conf["metaData"]["executionInfo"]
    spark_conf = execution_info["conf"]["common"]
    assert execution_info["stepDays"] == 10_000
    assert {key: spark_conf[key] for key in EXPECTED_SPARK_CONF} == EXPECTED_SPARK_CONF
    assert "range(0, 2000000, 1, 1024)" in compiled_conf["query"]
    assert "DISTRIBUTE BY shuffle_key" in compiled_conf["query"]

    workflow_id = submit_backfill(
        runner,
        chronon_root,
        hub_url,
        conf_path,
        START_DS,
        END_DS,
    )
    poll_workflow(hub_url, workflow_id, timeout=3600, interval=45)
