"""Hub TRAIN_MODEL CTR smoke test against AWS in-cluster Crucible/Ray."""

import json
import os
from pathlib import Path
from datetime import date, timedelta
from uuid import uuid4
from urllib.parse import urlparse

import boto3
import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill
from .helpers.workflow import poll_workflow

_COMPILE_ENV = os.environ.get("ZIPLINE_COMPILE_ENV", "prod").strip() or "prod"
_COMPILED_DIR = "compiled" if _COMPILE_ENV == "prod" else f"compiled_{_COMPILE_ENV}"
_MODEL_CONF = os.environ.get("ZIPLINE_CTR_MODEL_CONF")
_END_DS = os.environ.get("ZIPLINE_CTR_END_DS", "2026-08-24")


def _strict_loads(payload):
    """Reject NaN/Infinity: an artifact must parse for non-Python readers too."""
    def reject(constant):
        raise AssertionError(f"non-finite JSON constant in artifact: {constant}")
    return json.loads(payload, parse_constant=reject)


def _ctr_ray_e2e_enabled() -> bool:
    return os.environ.get("ZIPLINE_CTR_RAY_E2E", "").strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.integration
def test_hub_train_ctr_ray(chronon_root, hub_url, cloud):
    """Train on the real UC/Polaris labeled dataset through Hub and Ray."""
    if cloud != "aws":
        pytest.skip(f"Ray CTR training is AWS-only; cloud={cloud}")
    if not _ctr_ray_e2e_enabled():
        pytest.skip("Set ZIPLINE_CTR_RAY_E2E=1 to run the Hub CTR Ray e2e")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    invocation_id = uuid4().hex
    prefix = f"models/test_ctr_model_ray/e2e/{invocation_id}"
    if _MODEL_CONF:
        model_conf = _MODEL_CONF
    else:
        from models.aws.ctr_ray import ctr_ray_model
        model_conf = f"{_COMPILED_DIR}/models/aws/ctr_ray.ctr_ray_model__{ctr_ray_model.metaData.version}"
    source_conf = Path(chronon_root) / model_conf
    run_conf = source_conf.with_name(f"{source_conf.name}__e2e_{invocation_id}")
    compiled_model = json.loads(source_conf.read_text())
    # Hub indexes compiled configs by metadata name, not file path. Give this
    # invocation its own identity so the base config cannot win the hash-map
    # collision and silently restore the shared model-dir.
    compiled_model["metaData"]["name"] = f"models.aws.ctr_ray_e2e_{invocation_id}"
    job_configs = compiled_model["trainingConf"]["jobConfigs"]
    destination = urlparse(os.environ.get("ZIPLINE_CTR_ARTIFACT_BASE_URI", compiled_model["modelArtifactBaseUri"]))
    assert destination.scheme == "s3" and destination.netloc, "AWS E2E requires an s3 artifact base"
    prefix = "/".join(p for p in (destination.path.strip("/"), prefix) if p)
    bucket = destination.netloc
    job_configs["model-dir"] = f"s3://{bucket}/{prefix}"
    run_conf.write_text(json.dumps(compiled_model))

    run_conf_relative = str(run_conf.relative_to(chronon_root))
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-west-2")))
    artifact_keys = [
        f"{prefix}/model.joblib",
        f"{prefix}/metadata.json",
        f"{prefix}/experiment_report.json",
    ]
    try:
        training_workflow_id = submit_backfill(
            runner,
            chronon_root,
            hub_url,
            run_conf_relative,
            _END_DS,
            _END_DS,
            skip_compile=True,
        )
        poll_workflow(hub_url, training_workflow_id, timeout=1800, interval=30)

        for key in artifact_keys:
            artifact = s3.head_object(Bucket=bucket, Key=key)
            assert artifact["ContentLength"] > 0
        metadata = _strict_loads(s3.get_object(Bucket=bucket, Key=artifact_keys[1])["Body"].read())
        report = _strict_loads(s3.get_object(Bucket=bucket, Key=artifact_keys[2])["Body"].read())
        expected_features = [feature.split(":")[-1] for feature in job_configs["features"].split(",")]
        assert metadata["feature_names"] == expected_features
        assert metadata["num_features"] == len(expected_features)
        assert metadata["parameters"]["max_depth"] == int(job_configs["max-depth"])
        data = metadata["training_data"]
        source = compiled_model["trainingConf"]["trainingDataSource"]["events"]["table"].split("/")[0]
        assert data["input_table"] == source
        window = compiled_model["trainingConf"]["trainingDataWindow"]
        assert window["timeUnit"] == 1, "This E2E scenario expects a daily training window"
        assert data["start_ds"] == (date.fromisoformat(_END_DS) - timedelta(days=window["length"])).isoformat()
        assert data["end_ds"] == _END_DS
        assert metadata["training_data"]["row_count"] > 0
        assert metadata["training_data"]["start_ds"] <= metadata["training_data"]["observed_start_ds"]
        assert metadata["training_data"]["observed_end_ds"] <= _END_DS

        # The default multi-day window must actually score a holdout: a run that
        # silently fell back to training-only metrics is not a valid result here.
        split = report["split"]
        assert report["evaluation_status"] == "evaluated", report["evaluation_status"]
        assert split["train_end_ds"] < split["eval_start_ds"]
        assert split["eval_end_ds"] == metadata["training_data"]["observed_end_ds"]
        metrics = report["metrics"]
        for key in ("validation_auc", "validation_logloss", "validation_precision",
                    "training_rows", "validation_rows"):
            assert key in metrics, f"{key} missing from {sorted(metrics)}"
        assert metrics["training_rows"] + metrics["validation_rows"] == \
            metadata["training_data"]["row_count"]
        assert report["feature_names"] == expected_features
        assert report["precision_recall_curve"], "expected a precision-recall curve"
    finally:
        run_conf.unlink(missing_ok=True)
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in artifact_keys], "Quiet": True},
        )
