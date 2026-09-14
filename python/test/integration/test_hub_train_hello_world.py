"""Hub TRAIN_MODEL hello-world against AWS in-cluster Crucible/Ray."""

import os

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

_COMPILE_ENV = os.environ.get("ZIPLINE_COMPILE_ENV", "prod").strip() or "prod"
_COMPILED_DIR = "compiled" if _COMPILE_ENV == "prod" else f"compiled_{_COMPILE_ENV}"
_HELLO_WORLD_CONF = f"{_COMPILED_DIR}/models/aws_databricks/hello_world.hello_world_model__1.0"
_START_DS = "2026-02-01"
_END_DS = "2026-02-01"


def _ray_training_e2e_enabled() -> bool:
    return os.environ.get("ZIPLINE_RAY_TRAINING_E2E", "").strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.integration
def test_hub_train_hello_world(confs, chronon_root, hub_url, cloud):
    """Compile, submit TRAIN_MODEL through Hub, and poll to success."""
    if cloud != "aws":
        pytest.skip(f"Ray hello-world training is AWS-only; cloud={cloud}")
    if not _ray_training_e2e_enabled():
        pytest.skip("Set ZIPLINE_RAY_TRAINING_E2E=1 to run Hub Ray training e2e")

    runner = CliRunner()
    compile_configs(runner, chronon_root)
    workflow_id = submit_backfill(
        runner,
        chronon_root,
        hub_url,
        confs(_HELLO_WORLD_CONF),
        _START_DS,
        _END_DS,
    )
    poll_workflow(hub_url, workflow_id, timeout=900, interval=30)
