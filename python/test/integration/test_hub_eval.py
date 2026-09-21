"""Cloud-agnostic hub eval integration tests.

Exercises: compile -> upload diffs -> eval (schema validation and test-data evaluation).
"""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_eval

DEMO_JOIN = {
    "gcp": "compiled/joins/gcp/demo.v1__1",
    "aws": "compiled/joins/aws/demo.v1__1",
    "azure": "compiled/group_bys/azure/dim_listings.v3",
}

S3A_FILE_STAGING_QUERY = "compiled/staging_queries/aws/s3a_file_example.v1"


@pytest.mark.integration
def test_eval(confs, chronon_root, hub_url, eval_url, cloud):
    """Eval validates a conf against source tables and schemas."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    submit_eval(runner, chronon_root, hub_url, eval_url, confs(DEMO_JOIN[cloud]))


@pytest.mark.integration
def test_eval_s3a_file(confs, chronon_root, hub_url, eval_url, cloud):
    """AWS eval can read a Parquet file directly through the S3A filesystem."""
    if cloud != "aws":
        pytest.skip(f"S3A file eval is only supported for aws, skipping for cloud: {cloud}")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    submit_eval(
        runner,
        chronon_root,
        hub_url,
        eval_url,
        confs(S3A_FILE_STAGING_QUERY),
    )


@pytest.mark.integration
def test_eval_with_test_data(confs, chronon_root, hub_url, eval_url, cloud):
    """Eval with a test data YAML uses fixture rows instead of real source tables."""
    if cloud != "gcp":
        pytest.skip(f"test data eval is only supported for gcp, skipping for cloud: {cloud}")
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    test_data_path = os.path.join(chronon_root, "test_data", "exports.yaml")
    submit_eval(
        runner, chronon_root, hub_url, eval_url,
        confs(DEMO_JOIN[cloud]),
        test_data_path=test_data_path,
    )
