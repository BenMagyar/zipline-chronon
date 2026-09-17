"""Mandatory tests for the AWS CTR Ray trainer."""

import hashlib
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pytest

TRAINER_ROOT = (
    Path(__file__).parents[1] / "test" / "canary" / "model_examples" / "ctr_model_ray"
)


@pytest.fixture
def trainer_modules():
    """Import this example's top-level trainer package without leaking it globally."""
    previous = {
        name: module
        for name, module in sys.modules.items()
        if name == "trainer" or name.startswith("trainer.")
    }
    for name in previous:
        sys.modules.pop(name, None)
    sys.path.insert(0, str(TRAINER_ROOT))
    try:
        cli = importlib.import_module("trainer.cli")
        train = importlib.import_module("trainer.train")
        yield cli, train
    finally:
        sys.path.remove(str(TRAINER_ROOT))
        for name in list(sys.modules):
            if name == "trainer" or name.startswith("trainer."):
                sys.modules.pop(name, None)
        sys.modules.update(previous)


def test_parser_accepts_glue_arguments(monkeypatch, trainer_modules):
    cli, _ = trainer_modules
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    args = cli.parse_args(
        [
            "--catalog-region=us-west-2",
            "--catalog-type=glue",
            "--eta=0.1",
            "--max-depth=4",
            "--model-dir=/tmp/ctr",
            "--num-boost-round=50",
            "--input-table=data.aws_ctr_labels_v1__0",
            "--start-ds=2026-08-20",
            "--end-ds=2026-08-24",
        ]
    )
    assert args.input_table == "data.aws_ctr_labels_v1__0"
    assert args.start_ds == "2026-08-20"
    assert args.end_ds == "2026-08-24"
    assert cli.catalog_properties_from_args(args) == {
        "type": "glue",
        "s3.region": "us-west-2",
        "client.region": "us-west-2",
    }


def test_parser_builds_rest_catalog_properties_from_env(monkeypatch, trainer_modules):
    cli, _ = trainer_modules
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    # Only fake env-var *names* appear on the CLI; the values live in the env so
    # nothing sensitive is ever passed on the command line.
    monkeypatch.setenv("FAKE_DBX_CLIENT_ID", "fake-id")
    monkeypatch.setenv("FAKE_DBX_CLIENT_SECRET", "fake-secret")
    args = cli.parse_args(
        [
            "--catalog-type=rest",
            "--catalog-uri=https://example.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
            "--oauth2-server-uri=https://example.cloud.databricks.com/oidc/v1/token",
            "--warehouse=workspace",
            "--client-id-env=FAKE_DBX_CLIENT_ID",
            "--client-secret-env=FAKE_DBX_CLIENT_SECRET",
            "--vended-credentials=true",
            "--model-dir=/tmp/ctr",
            "--input-table=poc.aws_databricks_ctr_labels_v1__0",
            "--start-ds=2026-08-20",
            "--end-ds=2026-08-24",
        ]
    )
    assert cli.catalog_properties_from_args(args) == {
        "type": "rest",
        "warehouse": "workspace",
        "uri": "https://example.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest",
        "oauth2-server-uri": "https://example.cloud.databricks.com/oidc/v1/token",
        "scope": "all-apis",
        "credential": "fake-id:fake-secret",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
    }


def test_rest_catalog_credential_requires_env_values(monkeypatch, trainer_modules):
    cli, _ = trainer_modules
    monkeypatch.delenv("MISSING_DBX_ID", raising=False)
    monkeypatch.delenv("MISSING_DBX_SECRET", raising=False)
    args = cli.parse_args(
        [
            "--catalog-type=rest",
            "--catalog-uri=https://example/iceberg-rest",
            "--client-id-env=MISSING_DBX_ID",
            "--client-secret-env=MISSING_DBX_SECRET",
            "--model-dir=/tmp/ctr",
            "--input-table=poc.tbl",
            "--start-ds=2026-08-20",
            "--end-ds=2026-08-24",
        ]
    )
    with pytest.raises(ValueError, match="missing required env var"):
        cli.catalog_properties_from_args(args)


def test_model_dir_is_required_without_environment(monkeypatch, trainer_modules):
    cli, _ = trainer_modules
    monkeypatch.delenv("MODEL_DIR", raising=False)
    with pytest.raises(SystemExit):
        cli.parse_args(
            [
                "--input-table=example.table",
                "--start-ds=2026-08-20",
                "--end-ds=2026-08-24",
            ]
        )


def test_shape_training_frame_and_local_artifacts(tmp_path, trainer_modules):
    _, train = trainer_modules
    frame = pd.DataFrame(
        {
            "user_id_click_event_average_7d": [1.0, None],
            "listing_id_price_cents": [1000, 2000],
            "price_log": [6.9, 7.6],
            "price_bucket": [1, 2],
            "label": [0, 1],
        }
    )
    x_train, y_train = train.shape_training_frame(frame)
    assert list(x_train.columns) == train.FEATURE_COLUMNS
    assert x_train["user_id_click_event_average_7d"].tolist() == [1.0, 0.0]
    assert y_train == [0, 1]

    model = train.train_model(x_train, y_train, num_boost_round=2)
    output_dir = tmp_path / "artifacts"
    train.save_model(model, str(output_dir))
    assert (output_dir / "model.joblib").is_file()
    metadata = json.loads((output_dir / "metadata.json").read_text())
    assert metadata["feature_names"] == train.FEATURE_COLUMNS
    assert metadata["model_type"] == "xgboost"


def test_empty_scan_has_a_descriptive_error(trainer_modules):
    _, train = trainer_modules

    class EmptyScan:
        def to_arrow(self):
            return type("ArrowTable", (), {"to_pandas": lambda self: pd.DataFrame()})()

    class EmptyTable:
        def scan(self, **_kwargs):
            return EmptyScan()

    class Catalog:
        def load_table(self, _name):
            return EmptyTable()

    with pytest.raises(ValueError, match="no training rows found.*2026-08-24"):
        train.get_training_data_df(
            Catalog(),
            "data.empty",
            "2026-08-24",
            "2026-08-24",
        )


def test_archive_build_is_deterministic_and_contains_only_runtime_sources(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "ctr_ray_build_archive", TRAINER_ROOT / "build_archive.py"
    )
    builder = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(builder)

    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    builder._archive_bytes(first)
    original_zip_info = builder.ZipInfo
    def windows_zip_info(*args, **kwargs):
        info = original_zip_info(*args, **kwargs)
        info.create_system = 0
        return info
    monkeypatch.setattr(builder, "ZipInfo", windows_zip_info)
    builder._archive_bytes(second)

    assert hashlib.sha256(first.read_bytes()).digest() == hashlib.sha256(
        second.read_bytes()
    ).digest()
    with ZipFile(first) as archive:
        assert sorted(archive.namelist()) == [
            "setup.py",
            "trainer/__init__.py",
            "trainer/cli.py",
            "trainer/smoke_local.py",
            "trainer/train.py",
        ]


def test_remote_write_hard_exits_so_ray_driver_cannot_hang(monkeypatch, trainer_modules):
    _, train = trainer_modules
    exits = []
    monkeypatch.setattr(train.os, "_exit", lambda code: exits.append(code))

    # Local paths have no native S3 thread, so we exit normally (no hard exit).
    train.finalize_remote_filesystems("/tmp/local-model")
    assert exits == []

    # Remote paths must hard-exit(0): pyarrow.fs.finalize_s3() deadlocks on the
    # AWS SDK event-loop thread, so after the artifacts are already flushed we
    # bypass the hanging interpreter shutdown and force a successful exit.
    train.finalize_remote_filesystems("s3://bucket/model")
    train.finalize_remote_filesystems("s3a://bucket/model")
    assert exits == [0, 0]


def test_alternate_contract_and_metadata(tmp_path, trainer_modules):
    cli, train = trainer_modules
    features = cli.parse_features("new_price:price,rooms,area")
    frame = pd.DataFrame({"new_price": [1, 2, 3, 4], "rooms": [2, 3, 2, 4],
                          "area": [10, 20, 15, 25], "clicked": [0, 1, 0, 1]})
    x, y = train.shape_training_frame(frame, features, "clicked")
    assert list(x.columns) == ["price", "rooms", "area"]
    model = train.train_model(x, y, max_depth=6, seed=7, num_boost_round=2)
    train.save_model(model, str(tmp_path))
    metadata = json.loads((tmp_path / "metadata.json").read_text())
    assert metadata["feature_names"] == ["price", "rooms", "area"]
    assert metadata["num_features"] == 3
    assert metadata["parameters"]["max_depth"] == 6
    assert metadata["parameters"]["seed"] == 7
    config = json.loads(model.save_config())
    assert config["learner"]["gradient_booster"]["tree_train_param"]["max_depth"] == "6"


@pytest.mark.parametrize("features", ["", "a,a", "a:x,b:x", "a;bad", "a:b:c"])
def test_invalid_features(features, trainer_modules):
    import argparse
    cli, _ = trainer_modules
    with pytest.raises(argparse.ArgumentTypeError):
        cli.parse_features(features)


def test_half_configured_credential_rejected(trainer_modules):
    cli, _ = trainer_modules
    args = cli.parse_args(["--input-table=poc.t", "--start-ds=2026-08-20",
                           "--end-ds=2026-08-24", "--model-dir=/tmp/test",
                           "--catalog-type=rest", "--client-id-env=FAKE_ID"])
    with pytest.raises(ValueError, match="set both"):
        cli.catalog_properties_from_args(args)


def test_alternate_scan_projection_and_dates(trainer_modules):
    from pyiceberg.expressions import And, GreaterThanOrEqual, LessThanOrEqual
    import pyarrow as pa
    cli, train = trainer_modules
    captured = {}
    class Table:
        def scan(self, **kwargs):
            captured.update(kwargs)
            return self
        def to_arrow(self):
            return pa.Table.from_pydict({"area": [10, 20], "clicked": [0, 1],
                                         "day": ["2026-09-01", "2026-09-02"]})
    class Catalog:
        def load_table(self, name):
            captured["table"] = name
            return Table()
    x, y = train.get_training_data_df(Catalog(), "other.labels", "2026-09-01", "2026-09-02",
                                      cli.parse_features("area:size"), "clicked", "day")
    assert captured["table"] == "other.labels"
    assert captured["selected_fields"] == ("area", "day", "clicked")
    assert captured["row_filter"] == And(GreaterThanOrEqual("day", "2026-09-01"),
                                          LessThanOrEqual("day", "2026-09-02"))
    assert list(x.columns) == ["size"]
    assert y == [0, 1]
    assert x.attrs["training_data"]["row_count"] == 2
    assert x.attrs["training_data"]["observed_start_ds"] == "2026-09-01"


def test_spark_qualified_table_loads_by_rest_identifier(trainer_modules):
    import pyarrow as pa
    cli, train = trainer_modules
    loaded = []
    class Table:
        def scan(self, **_kwargs):
            return self
        def to_arrow(self):
            return pa.Table.from_pydict({"a": [1, 2], "label": [0, 1], "ds": ["2026-08-24"] * 2})
    class Catalog:
        def load_table(self, name):
            loaded.append(name)
            return Table()
    x, _ = train.get_training_data_df(Catalog(), "workspace_iceberg.poc.labels_v1__1",
                                      "2026-08-24", "2026-08-24", cli.parse_features("a"))
    train.get_training_data_df(Catalog(), "poc.labels", "2026-08-24", "2026-08-24",
                               cli.parse_features("a"))
    assert loaded == ["poc.labels_v1__1", "poc.labels"]
    # Metadata keeps the Spark name so it matches the Hub-linked upstream table.
    assert x.attrs["training_data"]["input_table"] == "workspace_iceberg.poc.labels_v1__1"


@pytest.mark.parametrize("labels", [[0, None], [0, 0.5], [0, 2]])
def test_invalid_binary_labels(labels, trainer_modules):
    cli, train = trainer_modules
    with pytest.raises(ValueError, match="binary"):
        train.shape_training_frame(pd.DataFrame({"a": [1, 2], "label": labels}), cli.parse_features("a"))


def test_nullable_boolean_features_train(trainer_modules):
    _, train = trainer_modules
    x, y = train.shape_training_frame(
        pd.DataFrame({"active": [True, None, False], "label": [1, 0, 0]}),
        [("active", "active")],
    )
    assert x["active"].tolist() == [1, 0, 0]
    model = train.train_model(x, y, num_boost_round=1)
    assert model.num_features() == 1
